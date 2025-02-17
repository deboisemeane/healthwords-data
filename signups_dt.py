import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.ensemble import RandomForestClassifier
from sklearn.tree import DecisionTreeClassifier, plot_tree
from sklearn.metrics import classification_report, roc_auc_score, confusion_matrix, ConfusionMatrixDisplay
from sklearn.inspection import partial_dependence, PartialDependenceDisplay



# Load preprocessed data
df = pd.read_csv("data/preprocessed/preprocessed_signups.csv")
df = df.dropna()  # Drop rows with missing values

# Train-test split
X = df.drop(columns=['signed_up'])  # Features
y = df['signed_up']                 # Target variable

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=43)

# Train Random Forest model
model = DecisionTreeClassifier(class_weight="balanced", random_state=43, max_depth=100)
model.fit(X_train, y_train)


X_eval = X_test
y_eval = y_test

# Predictions
y_pred = model.predict(X_eval)
y_prob = model.predict_proba(X_eval)[:, 1]

# Evaluate
print(classification_report(y_eval, y_pred))
print("AUC Score:", roc_auc_score(y_eval, y_prob))
ConfusionMatrixDisplay.from_predictions(y_eval, y_pred)
#plt.show()

# Feature importance
feature_importance = pd.DataFrame({'Feature': X.columns, 'Importance': model.feature_importances_})
feature_importance = feature_importance.sort_values(by="Importance", ascending=False)

# Plot feature importance
plt.figure(figsize=(10, 5))
plt.barh(feature_importance["Feature"], feature_importance["Importance"], color='skyblue')
plt.xlabel("Feature Importance")
plt.ylabel("Feature")
plt.title("Random Forest Feature Importance")
plt.gca().invert_yaxis()  # Highest importance at the top
#plt.show()

print(feature_importance)

features = ["total_engagement_sec"]  # Replace with your actual column name
#PartialDependenceDisplay.from_estimator(model, X_test, features, kind="average")
#plt.show()

plot_tree(model, feature_names=X.columns, filled=True, max_depth=5, fontsize=5)
plt.show()