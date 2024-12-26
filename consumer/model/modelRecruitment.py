import os
import pickle
import pandas as pd

def prepOneHotEncoder(df, col, pathPackages):
    oneHotEncoder = pickle.load(open(os.path.join(pathPackages, 'prep' + col + '.pkl'), 'rb'))
    dfOneHotEncoder = pd.DataFrame(oneHotEncoder.transform(df[[col]]),
                                   columns=[col + "_" + str(i+1) for i in range(len(oneHotEncoder.categories_[0]))])
    df = pd.concat([df.drop(col, axis=1), dfOneHotEncoder], axis=1)
    return df

def prepStandardScaler(df, col, pathPackages):
    scaler = pickle.load(open(os.path.join(pathPackages, 'prep' + col + '.pkl'), 'rb'))
    df[col] = scaler.transform(df[[col]])
    return df

def runModel(data,path):
    pathPackages = os.path.join(path, "packages")
    col = pickle.load(open(os.path.join(pathPackages, 'columnModelling.pkl'), 'rb'))
    df = pd.DataFrame(data, index=[0])
    df = df[col]

    # Preprocessing categorical columns
    categorical_cols = ['Gender', 'Position', 'Status']
    for col in categorical_cols:
        df = prepOneHotEncoder(df, col, pathPackages)

    # Preprocessing numerical columns
    numerical_cols = ['Age']
    for col in numerical_cols:
        df = prepStandardScaler(df, col, pathPackages)

    X = df.values
    model = pickle.load(open(os.path.join(pathPackages, 'modelRecruitment.pkl'), 'rb'))
    y = model.predict(X)[0]
    
    return "Hired" if y == 1 else "Not Hired"

if __name__ == "__main__":
    # Contoh data baru untuk diprediksi
    new_data = {
        'Gender': 'Male',
        'Age': 28,
        'Position': 'Data Engineer',
        'Status': 'Interviewed'
    }

    path = os.getcwd()
    prediction = runModel(new_data, path)
    print(f"Prediction: {prediction}")
