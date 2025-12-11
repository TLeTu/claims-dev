import argparse
import io
import logging
import random
import numpy as np
import pandas as pd
import torch
import torch.nn as nn
import torch.optim as optim
import mlflow
from PIL import Image
from pyspark.sql import SparkSession
from torch.utils.data import Dataset, DataLoader
from torchvision import models, transforms
from mlflow.models.signature import infer_signature
from sklearn.model_selection import train_test_split

# --- Configuration ---
APP_NAME = "Train_Car_Damage_Model"
DEFAULT_S3_PATH = "s3a://car-smart-claims/silver/training_images"
DEFAULT_TRACKING_URI = "http://mlflow-server:5001"
DEFAULT_EXPERIMENT = "Car_Damage_Classification"

# --- Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def set_seed(seed):
    random.seed(seed)
    np.random.seed(seed)
    torch.manual_seed(seed)
    if torch.cuda.is_available():
        torch.cuda.manual_seed_all(seed)
        torch.backends.cudnn.deterministic = True
        torch.backends.cudnn.benchmark = False

class CarDamageDataset(Dataset):
    def __init__(self, dataframe, label_map, transform=None):
        self.dataframe = dataframe
        self.label_map = label_map
        self.transform = transform

    def __len__(self):
        return len(self.dataframe)

    def __getitem__(self, idx):
        row = self.dataframe.iloc[idx]
        image = Image.open(io.BytesIO(row['content'])).convert('RGB')
        
        if self.transform:
            image = self.transform(image)
            
        return image, self.label_map[row['label']]

def get_spark_session():
    return (
        SparkSession.builder
        .appName(APP_NAME)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.driver.extraClassPath", "/opt/spark/jars/hadoop-aws.jar:/opt/spark/jars/aws-java-sdk-bundle.jar")
        .getOrCreate()
    )

def build_model(num_classes, device):
    logging.info("Initializing EfficientNet-B0 model...")
    model = models.efficientnet_b0(weights='EfficientNet_B0_Weights.DEFAULT')

    for param in model.parameters():
        param.requires_grad = False

    in_features = model.classifier[1].in_features
    model.classifier[1] = nn.Linear(in_features, num_classes)
    
    return model.to(device)

def main(args):
    set_seed(args.seed)
    device = torch.device("cuda:0" if torch.cuda.is_available() else "cpu")
    logging.info(f"Training on device: {device}")

    # Load Data via Spark
    spark = get_spark_session()
    logging.info(f"Loading data from {args.data_path}")
    df_pandas = spark.read.format("delta").load(args.data_path).select("content", "label").toPandas()
    spark.stop()

    # Prepare Labels
    unique_labels = df_pandas['label'].unique()
    label_map = {label: i for i, label in enumerate(unique_labels)}
    num_classes = len(unique_labels)
    logging.info(f"Detected classes: {list(unique_labels)}")

    # Prepare Datasets
    transform = transforms.Compose([
        transforms.Resize((args.img_size, args.img_size)),
        transforms.ToTensor(),
        transforms.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225])
    ])

    full_dataset = CarDamageDataset(df_pandas, label_map, transform=transform)
    train_idx, val_idx = train_test_split(
        range(len(full_dataset)), 
        test_size=0.2, 
        stratify=[l for _, l in full_dataset],
        random_state=args.seed
    )
    
    train_loader = DataLoader(torch.utils.data.Subset(full_dataset, train_idx), batch_size=args.batch_size, shuffle=True)
    val_loader = DataLoader(torch.utils.data.Subset(full_dataset, val_idx), batch_size=args.batch_size)

    # MLflow Training Loop
    mlflow.set_tracking_uri(args.tracking_uri)
    mlflow.set_experiment(args.experiment_name)

    with mlflow.start_run() as run:
        logging.info(f"Started MLflow Run: {run.info.run_id}")
        mlflow.log_params(vars(args))
        mlflow.log_dict(label_map, "label_map.json")

        model = build_model(num_classes, device)
        criterion = nn.CrossEntropyLoss()
        optimizer = optim.Adam(model.parameters(), lr=args.lr)

        for epoch in range(args.epochs):
            model.train()
            train_loss = 0.0
            for inputs, labels in train_loader:
                inputs, labels = inputs.to(device), labels.to(device)
                optimizer.zero_grad()
                loss = criterion(model(inputs), labels)
                loss.backward()
                optimizer.step()
                train_loss += loss.item()

            avg_train_loss = train_loss / len(train_loader)

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
            
            val_acc = 100 * correct / total

            mlflow.log_metric("train_loss", avg_train_loss, step=epoch)
            mlflow.log_metric("val_accuracy", val_acc, step=epoch)
            logging.info(f"Epoch {epoch+1}/{args.epochs} | Loss: {avg_train_loss:.4f} | Acc: {val_acc:.2f}%")

        # Log Model
        example_input, _ = next(iter(val_loader))
        signature = infer_signature(example_input.numpy(), model(example_input.to(device)).detach().cpu().numpy())
        
        mlflow.pytorch.log_model(model, "damage-classifier-model", signature=signature)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Train Car Damage Model")
    
    parser.add_argument("--tracking_uri", default=DEFAULT_TRACKING_URI)
    parser.add_argument("--experiment_name", default=DEFAULT_EXPERIMENT)
    parser.add_argument("--data_path", default=DEFAULT_S3_PATH)
    
    parser.add_argument("--epochs", type=int, default=5)
    parser.add_argument("--lr", type=float, default=0.001)
    parser.add_argument("--batch_size", type=int, default=32)
    parser.add_argument("--img_size", type=int, default=224)
    parser.add_argument("--seed", type=int, default=42)
    
    args = parser.parse_args()
    main(args)