import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import classification_report, roc_auc_score, confusion_matrix, ConfusionMatrixDisplay

# Load preprocessed data
df = pd.read_csv("data/preprocessed/preprocessed_signups.csv")
df = df.dropna() # Drop rows with missing values

# Train-test split
X = df.drop(columns=['signed_up'])  # Features
y = df['signed_up']                 # Target variable

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)


# Train logistic regression model
model = LogisticRegression(class_weight='balanced', penalty='l2')
model.fit(X_train, y_train)

# Predictions
y_pred = model.predict(X_test)
y_prob = model.predict_proba(X_test)[:, 1]

# Evaluate
print(classification_report(y_test, y_pred))
print("AUC Score:", roc_auc_score(y_test, y_prob))
ConfusionMatrixDisplay.from_predictions(y_test, y_pred)  # Create and display in one line
plt.show() 
# Feature importance (coefficients)
feature_importance = pd.DataFrame({'Feature': X.columns, 'Coefficient': model.coef_[0]})
print(feature_importance.sort_values(by="Coefficient", ascending=False))
