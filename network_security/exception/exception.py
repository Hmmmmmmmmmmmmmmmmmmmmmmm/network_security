import sys
from network_security.logging.logger import get_logger
log = get_logger(__name__)
def error_msg_details(error, error_detail:sys):
    '''
    Custom error msg formatter
    '''

    _,_,exc_tb = error_detail.exc_info()
    # file_name = exc_tb.tb_frame.f_code.co_filename
    if exc_tb is not None:
        file_name = exc_tb.tb_frame.f_code.co_filename
        line_no = exc_tb.tb_lineno
    else:
        file_name = "Unknown"
        line_no = "Unknown"
    # error_msg = f"Error occurred in python script name [{0}] line [{1}] error msg [{2}]"(
    #     file_name, exc_tb.tb_lineno, str(error)
    # )

    error_msg = (
        f"Error occurred in python script [{file_name}]"
        f"line [{line_no}] error message [{error}]"
    )

    return error_msg


class NetworkSecurityException(Exception):
    # def __init__(self, error_message, error_details:sys):
    #     self.error_message = error_message
    #     _,_,exc_tb = error_details.exc_info()

    #     self.lineno = exc_tb.tb_frame.f_code.co_filename

    # def __str__(self):
    #     return "Error occurred in python script name [{0}] line number [{1}] error message [{2}]".format(
    #         self.file_name, self.lineno, str(self.error_message))

    def __init__(self, error_msg, error_details:sys):
        super().__init__(error_msg)
        self.error_msg = error_msg_details(
            error=error_msg,
            error_detail=error_details
        )

    def __str__(self):
        return self.error_msg

if __name__=='__main__':
    try:
        log.info("Entered Try Block")
        a=10/0
        print("This wont be printed: ",a)
    except Exception as e:
        log.error("Divided by Zero")
        raise NetworkSecurityException(e,sys)