from datetime import datetime


class AppLogger:
    def __init__(self) -> None:
        pass

    def log(self, file_obj, log_msg):
        self.now = datetime.now()
        self.date = self.now.date()
        self.current_time = self.now.strftime("%H:%M:%S")

        try:
            file_obj.write(str(self.date) + " / " + str(self.current_time) + " : " + log_msg + "\n")
        except Exception as e:
            file_obj.write(f"Exception occured : {str(e)}")