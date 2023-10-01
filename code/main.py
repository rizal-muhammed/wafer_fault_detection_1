from training_validation_insertion import TrainValidation
from training_model import TrainModel
from prediction_validation_insertion import PredValidation
from predict_from_model import Prediction


if "__name__" == "__main__":
    train_path = "code/Training_Batch_Files/"
    train_val = TrainValidation(train_path)
    training = TrainModel()
    training.training_model()

    pred_path = "code/Prediction_Batch_files/"
    pred_val = PredValidation(pred_path)
    prediction = Prediction(pred_path)
    pred_output_path, sample_prediction = prediction.prediction_from_model()
    print(f"The predictions are available at the following path\n{str(pred_output_path)}")
    print(f"Sample prediction\n")
    print(sample_prediction)
