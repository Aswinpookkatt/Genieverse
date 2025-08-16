from utils import run_query
import logging


logger = logging.getLogger(__name__)


class DataQualityChecker:
    def __init__(self,schema):
        self.schema = schema

    
    def run_quality_checks(self):
        """Run comprehensive data quality checks"""
        
        all_issues = []


        for table_name in self.schema.keys():
            logger.info(f"Running quality checks on {table_name}")

            table_issues = self._check_table_quality(table_name)
            all_issues.extend(table_issues)

        return all_issues


    def _check_table_quality(self, table_name):
        """Check quality issues for a specific table"""

        issues = []

        #Checking for duplicate rows 
        issues.extend(self._check_duplicates(table_name))

        #Checking for referentialm integrity
        issues.extend(self._check_referential_integrity(table_name))

        #Checking for data consistency
        issues.extend(self._check_data_consistency(table_name))

        return issues

    
    def _check_duplicates(self, table_name):
        """Check for duplicate rows"""
        issues = []
        columns = self.schema.get(table_name,[])

        if columns:
            col_list = ', '.join(columns)
            query = f"""
            SELECT COUNT(*) as total_rows,
                COUNT(DISTINCT ({col_list})) as distinct_rows
                FROM {table_name}
            """

            result = run_query(query).iloc[0]

            if result['total_rows']!= result['distinct_rows']:
                duplicate_count = result['total_rows'] - result['distinct_rows']

                issues.append({
                    'type':'DUPLICATE_ROWS',
                    'table': table_name,
                    'message': f"Found {duplicate_count} duplicate rows in {table_name}"
                    
                })

        return issues


    def _check_referential_integrity(self, table_name):
        """Check for potential foriegn key violations"""
        issues = []
        columns = self.schema.get(table_name,[])

        #Look for columns that might be foreign keys
        potential_fks = [col for col in columns if col.endswith('_id')]

        for fk_col in potential_fks:

            #check if there are null in foriegn key
            query = f"""
            SELECT COUNT(*) as null_count
            FROM {table_name}
            WHERE {fk_col} IS NULL 
            """

            result = run_query(query).iloc[0]

            if result['null_count'] > 0 :
                issues.append({
                    'type' : 'POTENTIAL_FK_VIOLATION',
                    'table' : table_name,
                    'column' : fk_col,
                    'message' : f"Potential foreign key column {fk_col} has {result['null_count']} null values"
                }) 

        return issues

    
    def _check_data_consistency(self, table_name):
        """Check for data consistency issues"""
        issues = []

        columns = self.schema.get(table_name,[])

        for col in columns:
            #Check for inconsistent formatting in string columns
            if 'name' in col.lower() or 'email' in col.lower():
                issues.extend(self._check_string_consistency(table_name,col))

        
        return issues


    def _check_string_consistency(self, table_name, column_name):
        """Check string column consistency"""
        issues = []

        #Check for mixed case 
        query = f"""
        SELECT COUNT(DISTINCT UPPER({column_name}))
        as upper_distinct,
        COUNT(DISTINCT {column_name})
        as original_distinct
        FROM {table_name}
        WHERE {column_name} IS NOT NULL
        """

        result = run_query(query).iloc[0]

        if result['upper_distinct']!= result['original_distinct']:
            issues.append({
                'type':'INCONSISTENT_CASING',
                'table':table_name,
                'column':column_name,
                'message': f"Column {column_name} has inconsistent casing"
            })

        return issues