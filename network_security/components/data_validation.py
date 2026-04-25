from network_security.entity.artifact_entity import DataIngestionArtifact
from network_security.entity.artifact_entity import DataValidationArtifact
from network_security.entity.config_entity import DataValidationConfig
from network_security.exception.exception import NetworkSecurityException
from network_security.logging.logger import get_logger

log = get_logger(__name__)

from network_security.constant.training_pipeline import SCHEMA_FILE_PATH,DRIFT_THRESHOLD
from network_security.utils.main_utils.utils import read_yaml, write_yaml
import os, sys
from scipy.stats import ks_2samp, chi2_contingency
import pandas as pd
import numpy as np

class DataValidation:
    def __init__(self, data_ingestion_artifact: DataIngestionArtifact, data_validation_config: DataValidationConfig):
        try:
            log.info("Entered DataValidation Constructor")
            self.data_ingestion_artifact = data_ingestion_artifact
            self.data_validation_config = data_validation_config
            self._schema_config = read_yaml(SCHEMA_FILE_PATH)
        except Exception as e:
            # log.error("Failed during data ingestion", exc_info=True)
            raise NetworkSecurityException(e, sys) from e

    @staticmethod
    def read_data(file_path)->pd.DataFrame:
        try:
            return pd.read_csv(file_path)
        except Exception as e:
            # log.error("Failed during data validation", exc_info=True)
            raise NetworkSecurityException(e, sys) from e

    def validate_number_of_columns(self, dataframe: pd.DataFrame)->bool:
        try:
            number_of_columns = len(self._schema_config.get("columns",[]))
            existing_number_of_columns = len(dataframe.columns)
            log.info(f"Required number of columns: {number_of_columns}")
            log.info(f"Existing number of columns: {existing_number_of_columns}")
            return existing_number_of_columns==number_of_columns
        except Exception as e:
            # log.error("Failed during data validation", exc_info=True)
            raise NetworkSecurityException(e, sys) from e

    def numerical_columns_exist(self, df: pd.DataFrame) -> bool:
        di = self._schema_config
        # Build schema dict once
        column_schema = {k: v for item in di.get('columns', []) for k, v in item.items()}
        num_cols = di.get('numerical_columns', [])
        df_cols = df.columns
        # Check missing columns
        missing = np.setdiff1d(num_cols, df_cols)
        if missing.size > 0:
            log.info(f"Missing columns: {missing.tolist()}")
            return False

        # Check dtypes in one go
        actual_dtypes = df[num_cols].dtypes.astype(str)
        expected_dtypes = pd.Series({col: column_schema.get(col) for col in num_cols})
        mismatch_mask = actual_dtypes != expected_dtypes
        if mismatch_mask.any():
            mismatches = list(zip(
                actual_dtypes.index[mismatch_mask],
                expected_dtypes[mismatch_mask],
                actual_dtypes[mismatch_mask]
            ))
            for col, exp, act in mismatches:
                log.info(f"{col}: expected {exp}, got {act}")
            return False
        return True

    def detect_dataset_drift(self, base_df, current_df, threshold = DRIFT_THRESHOLD)->bool:
        try:
            status = True
            report = {}
            for column in base_df.columns:
                d1 = base_df[column]
                d2 = current_df[column]
                is_same_distribution = ks_2samp(d1,d2)
                if threshold<=is_same_distribution.pvalue:
                    is_found = False
                else:
                    is_found = True
                    status = False
                report.update({column:{
                    "p_value":float(is_same_distribution.pvalue),
                    "drift_status":is_found
                }})
            drift_report_file_path = self.data_validation_config.drift_report_file_path
            dir_path = os.path.dirname(drift_report_file_path)
            os.makedirs(dir_path,exist_ok=True)
            write_yaml(file_path=drift_report_file_path, content = report)
            return status
        except Exception as e:
            raise NetworkSecurityException(e, sys) from e
    def detect_dataset_drift_chi(self, base_df, current_df, threshold=DRIFT_THRESHOLD, allowed_drift_columns=2) -> bool:
        try:
            status = True
            report = {}
            drifted_column_count = 0

            for column in base_df.columns:
                # Frequency counts
                base_counts = base_df[column].value_counts()
                current_counts = current_df[column].value_counts()

                # Align categories (important!)
                all_categories = set(base_counts.index).union(set(current_counts.index))
                base_counts = base_counts.reindex(all_categories, fill_value=0)
                current_counts = current_counts.reindex(all_categories, fill_value=0)

                # Build contingency table
                contingency_table = [base_counts.values, current_counts.values]

                # Apply Chi-Square test
                chi2, p_value, _, _ = chi2_contingency(contingency_table)

                is_found = False

                if p_value < threshold:
                    is_found = True
                    drifted_column_count += 1

                report[column] = {
                    "p_value": float(p_value),
                    "drift_status": is_found
                }
            overall_status = drifted_column_count <= allowed_drift_columns

            report["summary"] = {
                "total_columns": len(base_df.columns),
                "drifted_columns": drifted_column_count,
                "allowed_drift_columns": allowed_drift_columns,
                "overall_status": overall_status
            }
            # Save report (FIXED: using config, not artifact)
            drift_report_file_path = self.data_validation_config.drift_report_file_path
            os.makedirs(os.path.dirname(drift_report_file_path), exist_ok=True)
            write_yaml(file_path=drift_report_file_path, content=report)

            return status

        except Exception as e:
            raise NetworkSecurityException(e, sys) from e

    def initiate_data_validation(self)->DataValidationArtifact:
        try:
            # read data
            train_file_path = self.data_ingestion_artifact.trained_file_path
            test_file_path = self.data_ingestion_artifact.test_file_path
            train_dataframe = DataValidation.read_data(train_file_path)
            test_dataframe = DataValidation.read_data(test_file_path)

            # Initialize tracking
            overall_status = True
            errors = []

            # Schema Validation
            # column count:
            if not self.validate_number_of_columns(dataframe=train_dataframe):
                overall_status = False
                errors.append("Train dataset does not contains all columns.")

            if not self.validate_number_of_columns(dataframe=test_dataframe):
                overall_status = False
                errors.append("Test dataset does not contains all columns.")
            # numerical column
            if not self.numerical_columns_exist(df = train_dataframe):
                overall_status = False
                errors.append("Train dataset does not contains all numeric columns.")
            if not self.numerical_columns_exist(df = test_dataframe):
                overall_status = False
                errors.append("Test dataset does not contains all numeric columns.")

            # column name consistency:
            if list(train_dataframe.columns) != list(test_dataframe.columns):
                overall_status = False
                errors.append("Train and Test datasets have mismatched columns.")

            # Drift!!!
            drift_status = self.detect_dataset_drift_chi(
                base_df=train_dataframe,
                current_df=test_dataframe
            )

            if not drift_status:
                overall_status = False
                errors.append("Data drift detected between train and test dataset\nRefer to report")

            # output setups:
            os.makedirs(os.path.dirname(self.data_validation_config.valid_train_file_path), exist_ok=True)
            os.makedirs(os.path.dirname(self.data_validation_config.invalid_train_file_path), exist_ok=True)

            # saving:
            if overall_status:
                train_dataframe.to_csv(
                    self.data_validation_config.valid_train_file_path,
                    index=False,header=True
                )
                test_dataframe.to_csv(
                    self.data_validation_config.valid_test_file_path,
                    index=False,header=True
                )
                valid_train_path = self.data_validation_config.valid_train_file_path
                valid_test_path = self.data_validation_config.valid_test_file_path
                invalid_train_path = None
                invalid_test_path = None

            else:
                train_dataframe.to_csv(
                    self.data_validation_config.invalid_train_file_path,
                    index=False,header=True
                )
                test_dataframe.to_csv(
                    self.data_validation_config.invalid_test_file_path,
                    index=False,header=True
                )

                valid_train_path = None
                valid_test_path = None
                invalid_train_path = self.data_validation_config.invalid_train_file_path
                invalid_test_path = self.data_validation_config.invalid_test_file_path
            # raising exceptions in cases of errors:
            if not overall_status:
                error_msg = " | ".join(errors)
                raise NetworkSecurityException(error_msg, sys)


            # # Validate number of columns:
            # status = self.validate_number_of_columns(dataframe=train_dataframe)
            # if not status:
            #     error_msg = f"Train dataset does not contains all columns.\n"
            # status = self.validate_number_of_columns(dataframe=test_dataframe)
            # if not status:
            #     error_msg = f"Test dataset does not contains all columns.\n"
            # status = self.numerical_columns_exist(df = train_dataframe)
            # if not status:
            #     error_msg = f"Train dataset does not contains all numeric columns.\n"
            # status = self.numerical_columns_exist(df = test_dataframe)
            # if not status:
            #     error_msg = f"Test dataset does not contains all numeric columns.\n"
            # # Checking data drift
            # status = self.detect_dataset_drift(
            #     base_df=train_dataframe,
            #     current_df=test_dataframe
            # )
            # dir_path = os.path.dirname(self.data_validation_config.valid_train_file_path)
            # os.makedirs(dir_path, exist_ok=True)
            # if not status:
            #     error_msg = f"Train dataset contains Drift!\n"
            # train_dataframe.to_csv(
            #     self.data_validation_config.valid_train_file_path, index=False, header=True
            # )
            # if not status:
            #     error_msg = f"Test dataset contains Drift!\n"
            # test_dataframe.to_csv(
            #     self.data_validation_config.valid_test_file_path, index=False, header=True
            # )
            data_validation_artifact = DataValidationArtifact(
                validation_status=overall_status,
                valid_train_file_path=self.data_validation_config.valid_train_file_path,
                valid_test_file_path=self.data_validation_config.valid_test_file_path,
                invalid_train_file_path=None,
                invalid_test_file_path=None,
                drift_report_file_path=self.data_validation_config.drift_report_file_path,
            )

            return data_validation_artifact
        except Exception as e:
            log.error("Failed during data validation", exc_info=True)
            raise NetworkSecurityException(e, sys) from e



