import pandas as pd 
import numpy as np 
from utils import run_query
import logging


logger = logging.getLogger(__name__)


class DataProfiler:
    def __init__(self, schema):
        self.schema = schema
        self.profile_results = {}

    def profile_table(self, table_name):
        """Generate comprehensive table profile"""
        logger.info(f"Profiling table: {table_name}")

        profile = {
            'table_name' : table_name,
            'row_count' : self._get_row_count(table_name),
            'column_profiles' : {},
            'anomalies' : []
        }

        columns = self.schema.get(table_name,[])

        for col in columns:
            profile['column_profiles'][col] = self._profiler_column(table_name, col)

        profile['anomalies'] = self._detect_anomalies(table_name, profile)
        self.profile_results[table_name] = profile

        return profile


    def _get_row_count(self,table_name):
        """Get total row count"""
        query = f"SELECT COUNT(*) as row_count FROM {table_name}"
        result = run_query(query)

        return result.iloc[0]['row_count']

    def _profiler_column(self, table_name, column_name):
        """Profile Individual Column"""
        try :
            # Basic stats
            query = f"""
            SELECT  COUNT(*) as total_count,
            COUNT({column_name}) as non_null_count,
            COUNT(DISTINCT {column_name}) as distinct_count
            FROM {table_name} """

            basic_stats = run_query(query).iloc[0]

            # Null percentage
            null_pct = (basic_stats['total_count'] - basic_stats['non_null_count']) / basic_stats['total_count'] * 100
            
            profile = {
                'total_count' : basic_stats['total_count'],
                'non_null_count':basic_stats['non_null_count'],
                'distinct_count': basic_stats['distinct_count'],
                'null_percentage':null_pct,
                'data_type': self._infer_data_type(table_name,column_name)
            }


            #Numeric column stats
            if profile['data_type'] in ['numeric','integer']:
                profile.update(self._get_numeric_stats(table_name,column_name))
                
                return profile

        except Exception as e:
            logger.error(f"Error profiling column {column_name}:{e}")

            return {'error':str(e)}

    
    def _infer_data_type(self, table_name, column_name):
        """Infer column data type"""
        query = f"DESCRIBE TABLE {table_name}"

        desc = run_query(query)
        col_info = desc[desc['column_name']==column_name]

        if not col_info.empty:
            data_type = col_info.iloc[0]['column_type'].lower()
            if any(x in data_type for x in ['int','bigint','smallint']):
                return 'integer'
            elif any(x in data_type for x in ['double','float','decimal']):
                return 'numeric'
            elif any(x in data_type for x in ['string', 'varchar', 'text']):
                return 'string'
            elif any(x in data_type for x in ['date', 'timestamp']):
                return 'datetime'
            return 'unknown'

    def _get_numeric_stats(self, table_name, column_name):
        """Get statistics for numeric columns"""
        query = f"""
        SELECT 
            MIN({column_name}) as min_val,
            MAX({column_name}) as max_val,
            AVG({column_name}) as mean_val,
            STDDEV({column_name}) as std_dev
        FROM {table_name}
        WHERE {column_name} is NOT NULL """

        stats = run_query(query).iloc[0]

        return {
            'min_value': stats['min_val'],
            'max_value': stats['max_val'],
            'mean_value': stats['mean_val'],
            'std_deviation': stats['std_dev']
            }
    
    def _detect_anomalies(self, table_name, profile):

        """Detect data anomalies"""
        anomalies = []

        # Check for high null rates
        for col, col_profile in profile['column_profiles'].items():
            if not isinstance(col_profile, dict):
                continue  # Skip if col_profile is None or not a dict
            if col_profile.get('null_percentage',0) > 50:
                anomalies.append({
                    'type':'HIGH_NULL_RATE',
                    'column':col,
                    'message':f"Column {col} has {col_profile['null_percentage']:.1f}% null values"
                })

        # Check for low cardinality
        for col, col_profile in profile['column_profiles'].items():
            if not isinstance(col_profile, dict):
                continue  # Skip if col_profile is None or not a dict
            if col_profile.get('data_type') == 'string':
                if col_profile.get('distinct_count',0)<5:
                    anomalies.append({
                        'type':'LOW_CARDINALITY',
                        'column':col,
                        'message':f"Column {col} has only {col_profile['distinct_count']} unique values"
                    })

        # Check for potential outliers in numeric columns
        for col, col_profile in profile['column_profiles'].items():
            if not isinstance(col_profile, dict):
                continue  # Skip if col_profile is None or not a dict
            if col_profile.get('data_type') in ['numeric','integer']:
                if col_profile.get('std_deviation',0)>0:
                    anomalies.extend(self._detect_outliers(table_name,col,col_profile))

        return anomalies

    
    def _detect_outliers(self, table_name,column_name,col_profile):
        """Detect statistical outliers"""
        anomalies = []
        mean = col_profile.get('mean_value',0)
        std = col_profile.get('std_deviation',0)

        if std>0:
            # Z score calc

            query = f"""
            SELECT COUNT(*) as outlier_count
            FROM {table_name}
            WHERE {column_name} IS NOT NULL
            AND (ABS({column_name}-{mean})/{std} ) > 3 """

            result = run_query(query)

            outlier_count = result.iloc[0]['outlier_count']

            if outlier_count > 0:
                anomalies.append({
                    'type':'STATISTICAL_OUTLIERS',
                    'column': column_name,
                    'message': f'Found {outlier_count} statistical outliers in {column_name}'
                })

        return anomalies