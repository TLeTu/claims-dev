import os
import mlflow
import pandas as pd
from PIL import Image
import io
import argparse
import logging
import random
import numpy as np

import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import Dataset, DataLoader, random_split
from torchvision import models, transforms
from mlflow.models.signature import infer_signature
from sklearn.model_selection import train_test_split

from pyspark.sql import SparkSession

# --- Setup Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def set_seed(seed_value):
    """Set seed for reproducibility."""
    random.seed(seed_value)
    np.random.seed(seed_value)
    torch.manual_seed(seed_value)
    if torch.cuda.is_available():
        torch.cuda.manual_seed_all(seed_value)
        torch.backends.cudnn.deterministic = True
        torch.backends.cudnn.benchmark = False

# =============================================================================
# 1. DATA HANDLING CLASSES AND FUNCTIONS
# =============================================================================

class CarDamageDataset(Dataset):
    """Custom PyTorch Dataset for loading car damage images."""
    def __init__(self, dataframe, label_map, transform=None):
        self.dataframe = dataframe
        self.label_map = label_map
        self.transform = transform

    def __len__(self):
        return len(self.dataframe)

    def __getitem__(self, idx):
        img_content = self.dataframe.iloc[idx]['content']
        label_str = self.dataframe.iloc[idx]['label']
        
        # Convert binary to a PIL Image, and ensure it's RGB
        image = Image.open(io.BytesIO(img_content)).convert('RGB')
        
        # Apply transformations
        if self.transform:
            image = self.transform(image)
            
        # Convert label string to integer
        label = self.label_map[label_str]
        
        return image, label

# =============================================================================
# 2. MAIN TRAINING LOGIC
# =============================================================================

def main(args):
    """Main function to run the model training and logging pipeline."""
    # Set the seed for reproducibility
    set_seed(args.random_seed)
    logging.info(f"Random seed set to {args.random_seed}")

    # --- MLflow Configuration ---
    logging.info("Setting up MLflow tracking...")
    mlflow.set_tracking_uri(args.mlflow_tracking_uri)
    mlflow.set_experiment(args.mlflow_experiment_name)

    # --- Spark Session ---
    logging.info("Initializing Spark session with Delta Lake and S3 support...")
    spark = SparkSession.builder \
        .appName("ML_Model_Training") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.driver.extraClassPath", "/opt/spark/jars/hadoop-aws.jar:/opt/spark/jars/aws-java-sdk-bundle.jar") \
        .getOrCreate()

    # --- Data Loading and Preparation ---
    logging.info(f"Loading data from {args.silver_data_path}...")
    silver_df = spark.read.format("delta").load(args.silver_data_path)

    logging.info("Converting Spark DataFrame to Pandas DataFrame...")
    images_pdf = silver_df.select("content", "label").toPandas()

    # Get unique labels and create a mapping to integers
    labels = images_pdf['label'].unique()
    label_to_int = {label: i for i, label in enumerate(labels)}
    int_to_label = {i: label for i, label in enumerate(labels)}
    num_classes = len(labels)
    logging.info(f"Found {num_classes} classes: {list(labels)}")

    # Define image transformations
    data_transforms = transforms.Compose([
        transforms.Resize((args.image_size, args.image_size)),
        transforms.ToTensor(),
        transforms.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225])
    ])

    # Instantiate the full dataset
    full_dataset = CarDamageDataset(images_pdf, label_to_int, transform=data_transforms)
    # Create indices for stratified split to ensure balanced classes in train/val sets
    targets = [label for _, label in full_dataset]
    train_indices, val_indices = train_test_split(
        range(len(full_dataset)),
        test_size=0.2,
        random_state=args.random_seed,
        stratify=targets
    )    
    # Create subsets for training and validation using the stratified indices
    train_dataset = torch.utils.data.Subset(full_dataset, train_indices)
    val_dataset = torch.utils.data.Subset(full_dataset, val_indices)

    # Create DataLoaders
    train_loader = DataLoader(train_dataset, batch_size=args.batch_size, shuffle=True)
    val_loader = DataLoader(val_dataset, batch_size=args.batch_size)
    logging.info(f"Created {len(train_dataset)} training samples and {len(val_dataset)} validation samples.")

    # --- MLflow Run ---
    logging.info("Starting MLflow Run for Model Training...")
    with mlflow.start_run() as run:
        logging.info(f"MLflow Run ID: {run.info.run_id}")
        
        # Log parameters from arguments
        mlflow.log_params(vars(args))
        mlflow.log_dict(int_to_label, "label_map.json")

        # --- Prepare the model ---
        logging.info(f"Loading pre-trained {args.base_model} model...")
        # Dynamically load the specified base model
        if args.base_model.lower() == 'efficientnet_b0':
            model = models.efficientnet_b0(weights='EfficientNet_B0_Weights.DEFAULT')
            # Replace the classifier
            in_features = model.classifier[1].in_features
            model.classifier[1] = nn.Linear(in_features, num_classes)
        else:
            # Example for another model, can be expanded
            # For resnet, the classifier layer is named 'fc'
            model = models.resnet50(weights='ResNet50_Weights.DEFAULT')
            in_features = model.fc.in_features
            model.fc = nn.Linear(in_features, num_classes)

        # Freeze all the parameters in the feature extractor
        for param in model.parameters():
            param.requires_grad = False
        
        device = torch.device("cuda:0" if torch.cuda.is_available() else "cpu")
        model = model.to(device)
        logging.info(f"Model moved to device: {device}")

        # --- Define loss function and optimizer ---
        criterion = nn.CrossEntropyLoss()
        optimizer = optim.Adam(filter(lambda p: p.requires_grad, model.parameters()), lr=args.learning_rate)

        # --- Training loop ---
        logging.info("Starting model training loop...")
        for epoch in range(args.epochs):
            # Training phase
            model.train()
            running_loss = 0.0
            for i, (inputs, labels) in enumerate(train_loader):
                inputs, labels = inputs.to(device), labels.to(device)
                optimizer.zero_grad()
                outputs = model(inputs)
                loss = criterion(outputs, labels)
                loss.backward()
                optimizer.step()
                running_loss += loss.item()
            avg_train_loss = running_loss / len(train_loader)
            mlflow.log_metric("training_loss", avg_train_loss, step=epoch)

            # Validation phase
            model.eval()
            correct = 0
            total = 0
            with torch.no_grad():
                for inputs, labels in val_loader:
                    inputs, labels = inputs.to(device), labels.to(device)
                    outputs = model(inputs)
                    _, predicted = torch.max(outputs.data, 1)
                    total += labels.size(0)
                    correct += (predicted == labels).sum().item()
            val_accuracy = 100 * correct / total
            mlflow.log_metric("validation_accuracy", val_accuracy, step=epoch)
            
            logging.info(f"Epoch {epoch+1}/{args.epochs} | Train Loss: {avg_train_loss:.4f} | Val Accuracy: {val_accuracy:.2f}%")

        # --- Logging the Final Model ---
        logging.info("Logging the final model to MLflow...")
        
        # Get a sample input batch to define the model signature
        input_example, _ = next(iter(val_loader))
        # Infer signature from a single input example and model output
        model.eval()
        with torch.no_grad():
            output_example = model(input_example.to(device))
        signature = infer_signature(input_example.numpy(), output_example.cpu().numpy())

        mlflow.pytorch.log_model(
            pytorch_model=model,
            artifact_path="damage-classifier-model",
            signature=signature
        )
        logging.info("Model successfully logged with signature.")

    logging.info("Training script finished successfully.")
    spark.stop()

# =============================================================================
# 3. SCRIPT EXECUTION
# =============================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PyTorch Model Training Script for Car Damage Classification")
    
    # --- MLflow Arguments ---
    parser.add_argument("--mlflow_tracking_uri", type=str, default="http://mlflow-server:5001", help="MLflow tracking server URI.")
    parser.add_argument("--mlflow_experiment_name", type=str, default="Car_Damage_Classification", help="Name of the MLflow experiment.")
    
    # --- Data Path Arguments ---
    parser.add_argument("--silver_data_path", type=str, default="s3a://car-smart-claims/silver/training_images", help="Path to the silver training images delta table.")
    
    # --- Hyperparameter Arguments ---
    parser.add_argument("--base_model", type=str, default="efficientnet_b0", help="Base model architecture (e.g., 'efficientnet_b0').")
    parser.add_argument("--epochs", type=int, default=5, help="Number of training epochs.")
    parser.add_argument("--learning_rate", type=float, default=0.001, help="Learning rate for the optimizer.")
    parser.add_argument("--batch_size", type=int, default=32, help="Batch size for training and validation.")
    parser.add_argument("--image_size", type=int, default=224, help="Image size (width and height).")
    parser.add_argument("--random_seed", type=int, default=42, help="Random seed for reproducibility.")
    
    args = parser.parse_args()
    main(args)