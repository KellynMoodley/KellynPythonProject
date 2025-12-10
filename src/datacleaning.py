#import packages
import pandas as pd
import uuid
import re
from typing import Tuple, List, Dict
import json


class DataCleaner:

    #Handles data cleaning, validation, and exclusion tracking for the dataset.
    
    def __init__(self):
        self.excluded_rows = []
        self.original_count = 0
        self.included_count = 0
        self.excluded_count = 0
    
    def add_row_id(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add unique row_id (UUID) to each row for tracking.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with row_id column added as first column
        """
        df['row_id'] = [str(uuid.uuid4()) for _ in range(len(df))]
        # Move row_id to first column
        cols = ['row_id'] + [col for col in df.columns if col != 'row_id']
        df = df[cols]
        return df
    
    def validate_name(self, name) -> Tuple[bool, str]:
        """
        Validate name contains only English letters and spaces.
        
        Args:
            name: Name value to validate
            
        Returns:
            Tuple of (is_valid, exclusion_reason)
        """
        # Check if name is missing or empty
        if pd.isna(name) or str(name).strip() == '':
            return False, "missing name"
        
        name_str = str(name).strip()
        
        # Check minimum length
        if len(name_str) < 3:
            return False, "name too short"
        
        # Check for valid characters (only A-Z, a-z, and spaces)
        if not re.match(r'^[A-Za-z ]+$', name_str):
            return False, "special character in name"
        
        return True, ""
    
    def validate_numeric_field(self, value, field_name: str) -> Tuple[bool, str]:
        """
        Validate that a field is numeric.
        
        Args:
            value: Value to validate
            field_name: Name of the field for error messages
            
        Returns:
            Tuple of (is_valid, exclusion_reason)
        """
        if pd.isna(value):
            return False, f"missing {field_name}"
        
        try:
            num_value = float(value)
            # Check if it's actually an integer (no decimal part)
            if num_value != int(num_value):
                return False, f"invalid {field_name} (not integer)"
            return True, ""
        except (ValueError, TypeError):
            return False, f"invalid {field_name} (not numeric)"
    
    def validate_day(self, day) -> Tuple[bool, str]:
        """
        Validate birth_day is numeric and in range 1-31.
        
        Args:
            day: Day value to validate
            
        Returns:
            Tuple of (is_valid, exclusion_reason)
        """
        is_numeric, reason = self.validate_numeric_field(day, "birth_day")
        if not is_numeric:
            return False, reason
        
        day_int = int(float(day))
        if day_int < 1 or day_int > 31:
            return False, "invalid day (not 1-31)"
        
        return True, ""
    
    def validate_month(self, month) -> Tuple[bool, str]:
        """
        Validate birth_month is numeric and in range 1-12.
        
        Args:
            month: Month value to validate
            
        Returns:
            Tuple of (is_valid, exclusion_reason)
        """
        is_numeric, reason = self.validate_numeric_field(month, "birth_month")
        if not is_numeric:
            return False, reason
        
        month_int = int(float(month))
        if month_int < 1 or month_int > 12:
            return False, "invalid month (not 1-12)"
        
        return True, ""
    
    def validate_year(self, year) -> Tuple[bool, str]:
        """
        Validate birth_year is numeric and >= 1940.
        
        Args:
            year: Year value to validate
            
        Returns:
            Tuple of (is_valid, exclusion_reason)
        """
        is_numeric, reason = self.validate_numeric_field(year, "birth_year")
        if not is_numeric:
            return False, reason
        
        year_int = int(float(year))
        if year_int < 1940:
            return False, "Birth year older than 1940"
        
        return True, ""
    
    def validate_row(self, row: pd.Series) -> Tuple[bool, List[str]]:
        """
        Validate all fields in a row.
        Priority order: name -> day -> month -> year
        
        Args:
            row: DataFrame row to validate
            
        Returns:
            Tuple of (is_valid, list_of_reasons)
        """
        reasons = []
        
        # Validate name first
        name_valid, name_reason = self.validate_name(row.get('name'))
        if not name_valid:
            reasons.append(name_reason)
            # If name fails, still check other fields
        
        # Validate day
        day_valid, day_reason = self.validate_day(row.get('birth_day'))
        if not day_valid:
            reasons.append(day_reason)
        
        # Validate month
        month_valid, month_reason = self.validate_month(row.get('birth_month'))
        if not month_valid:
            reasons.append(month_reason)
        
        # Validate year
        year_valid, year_reason = self.validate_year(row.get('birth_year'))
        if not year_valid:
            reasons.append(year_reason)
        
        is_valid = len(reasons) == 0
        return is_valid, reasons
    
    def clean_data(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Clean the dataset according to all validation rules.
        
        Args:
            df: Input DataFrame with columns: name, birth_day, birth_month, birth_year
            
        Returns:
            Tuple of (included_df, excluded_df)
        """
        # Store original count
        self.original_count = len(df)
        
        # Add row_id to track each row
        df = self.add_row_id(df)
        
        # Lists to store included and excluded rows
        included_rows = []
        excluded_rows = []
        
        # Validate each row
        for idx, row in df.iterrows():
            is_valid, reasons = self.validate_row(row)
            
            if is_valid:
                # Clean and add to included
                cleaned_row = {
                    'row_id': row['row_id'],
                    'name': str(row['name']).strip(),
                    'birth_day': int(float(row['birth_day'])),
                    'birth_month': int(float(row['birth_month'])),
                    'birth_year': int(float(row['birth_year']))
                }
                included_rows.append(cleaned_row)
            else:
                # Add to excluded with reasons
                excluded_row = {
                    'row_id': row['row_id'],
                    'name': row['name'] if pd.notna(row['name']) else '',
                    'birth_day': row['birth_day'] if pd.notna(row['birth_day']) else '',
                    'birth_month': row['birth_month'] if pd.notna(row['birth_month']) else '',
                    'birth_year': row['birth_year'] if pd.notna(row['birth_year']) else '',
                    'exclusion_reason': '; '.join(reasons)  # Join multiple reasons
                }
                excluded_rows.append(excluded_row)
        
        # Create DataFrames
        included_df = pd.DataFrame(included_rows)
        excluded_df = pd.DataFrame(excluded_rows)
        
        # Update counts
        self.included_count = len(included_df)
        self.excluded_count = len(excluded_df)
        
        return included_df, excluded_df
    
    def get_summary_stats(self, included_df: pd.DataFrame, excluded_df: pd.DataFrame) -> Dict:
        """
        Calculate summary statistics for the dataset.
        
        Args:
            included_df: DataFrame of included rows
            excluded_df: DataFrame of excluded rows
            
        Returns:
            Dictionary with summary statistics
        """
        total_count = self.original_count
        included_count = len(included_df)
        excluded_count = len(excluded_df)
        
        # Calculate percentages
        pct_included = (included_count / total_count * 100) if total_count > 0 else 0
        pct_excluded = (excluded_count / total_count * 100) if total_count > 0 else 0
        
        # Uniqueness metrics (only for included data)
        unique_names = included_df['name'].nunique() if not included_df.empty else 0
        
        # Unique birthday combinations
        if not included_df.empty:
            unique_birthdays = included_df.groupby(
                ['birth_day', 'birth_month', 'birth_year']
            ).size().shape[0]
            
            # Name + birth_year
            unique_name_year = included_df.groupby(
                ['name', 'birth_year']
            ).size().shape[0]
            
            # Name + birth_month
            unique_name_month = included_df.groupby(
                ['name', 'birth_month']
            ).size().shape[0]
            
            # Name + birth_day
            unique_name_day = included_df.groupby(
                ['name', 'birth_day']
            ).size().shape[0]
        else:
            unique_birthdays = 0
            unique_name_year = 0
            unique_name_month = 0
            unique_name_day = 0
        
        # Find duplicates (at least 2 of 4 fields match)
        duplicate_records = self.find_duplicate_records(included_df)
        
        summary = {
            'dataset_sizes': {
                'original_row_count': total_count,
                'included_row_count': included_count,
                'excluded_row_count': excluded_count,
                'pct_included_vs_original': round(pct_included, 2),
                'pct_excluded_vs_original': round(pct_excluded, 2)
            },
            'uniqueness': {
                'total_unique_names': unique_names,
                'unique_birthday_combinations': unique_birthdays,
                'unique_name_year_combinations': unique_name_year,
                'unique_name_month_combinations': unique_name_month,
                'unique_name_day_combinations': unique_name_day
            },
            'duplicates': duplicate_records
        }
        
        return summary
    
    def find_duplicate_records(self, df: pd.DataFrame) -> Dict:
        """
        Find records where at least 2 of 4 fields match.
        
        Args:
            df: DataFrame to analyze
            
        Returns:
            Dictionary with duplicate analysis
        """
        if df.empty:
            return {
                'total_duplicate_groups': 0,
                'total_duplicate_records': 0,
                'duplicate_groups': []
            }
        
        duplicate_groups = []
        processed_rows = set()
        
        # Check all possible 2-field combinations
        field_combinations = [
            ('name', 'birth_day'),
            ('name', 'birth_month'),
            ('name', 'birth_year'),
            ('birth_day', 'birth_month'),
            ('birth_day', 'birth_year'),
            ('birth_month', 'birth_year')
        ]
        
        for fields in field_combinations:
            grouped = df.groupby(list(fields))
            for group_key, group_df in grouped:
                if len(group_df) > 1:
                    # Found duplicates
                    row_ids = group_df['row_id'].tolist()
                    
                    # Only add if we haven't processed this exact group before
                    row_ids_set = frozenset(row_ids)
                    if row_ids_set not in processed_rows:
                        processed_rows.add(row_ids_set)
                        
                        duplicate_groups.append({
                            'matching_fields': list(fields),
                            'matching_values': {fields[i]: group_key[i] if isinstance(group_key, tuple) else group_key 
                                              for i in range(len(fields))},
                            'count': len(group_df),
                            'row_ids': row_ids
                        })
        
        # Count total duplicate records (unique row_ids involved in any duplicate group)
        all_duplicate_row_ids = set()
        for group in duplicate_groups:
            all_duplicate_row_ids.update(group['row_ids'])
        
        return {
            'total_duplicate_groups': len(duplicate_groups),
            'total_duplicate_records': len(all_duplicate_row_ids),
            'duplicate_groups': duplicate_groups
        }


def load_and_clean_data(csv_filepath: str) -> Tuple[pd.DataFrame, pd.DataFrame, Dict]:
    """
    Main function to load and clean data from CSV file.
    
    Args:
        csv_filepath: Path to the CSV file
        
    Returns:
        Tuple of (included_df, excluded_df, summary_stats)
    """
    # Load the CSV
    df = pd.read_csv(csv_filepath)
    
    # Initialize cleaner
    cleaner = DataCleaner()
    
    # Clean the data
    included_df, excluded_df = cleaner.clean_data(df)
    
    # Get summary statistics
    summary_stats = cleaner.get_summary_stats(included_df, excluded_df)
    
    return included_df, excluded_df, summary_stats


def save_reports(included_df: pd.DataFrame, excluded_df: pd.DataFrame, 
                summary_stats: Dict, output_dir: str = '.'):
    """
    Save the cleaning reports to files.
    
    Args:
        included_df: DataFrame of included rows
        excluded_df: DataFrame of excluded rows
        summary_stats: Dictionary of summary statistics
        output_dir: Directory to save reports (default: current directory)
    """
    import os
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Save included data
    included_path = os.path.join(output_dir, 'data_included.csv')
    included_df.to_csv(included_path, index=False)
    print(f"✓ Saved included data to: {included_path}")
    
    # Save excluded data
    excluded_path = os.path.join(output_dir, 'data_excluded.csv')
    excluded_df.to_csv(excluded_path, index=False)
    print(f"✓ Saved excluded data to: {excluded_path}")
    
    # Save summary statistics
    summary_path = os.path.join(output_dir, 'summary_stats.json')
    with open(summary_path, 'w') as f:
        json.dump(summary_stats, f, indent=2)
    print(f"✓ Saved summary statistics to: {summary_path}")
    
    # Print summary to console
    print("\n" + "="*60)
    print("DATA CLEANING SUMMARY")
    print("="*60)
    print(f"Original rows: {summary_stats['dataset_sizes']['original_row_count']}")
    print(f"Included rows: {summary_stats['dataset_sizes']['included_row_count']} "
          f"({summary_stats['dataset_sizes']['pct_included_vs_original']}%)")
    print(f"Excluded rows: {summary_stats['dataset_sizes']['excluded_row_count']} "
          f"({summary_stats['dataset_sizes']['pct_excluded_vs_original']}%)")
    print("\nUniqueness Metrics:")
    print(f"  Unique names: {summary_stats['uniqueness']['total_unique_names']}")
    print(f"  Unique birthdays: {summary_stats['uniqueness']['unique_birthday_combinations']}")
    print(f"  Unique name+year: {summary_stats['uniqueness']['unique_name_year_combinations']}")
    print(f"  Unique name+month: {summary_stats['uniqueness']['unique_name_month_combinations']}")
    print(f"  Unique name+day: {summary_stats['uniqueness']['unique_name_day_combinations']}")
    print("\nDuplicate Analysis:")
    print(f"  Duplicate groups: {summary_stats['duplicates']['total_duplicate_groups']}")
    print(f"  Records involved in duplicates: {summary_stats['duplicates']['total_duplicate_records']}")
    print("="*60)


# Example usage
if __name__ == "__main__":
    import sys
    
    # Check if CSV file path is provided
    if len(sys.argv) < 2:
        print("Usage: python datacleaning.py <csv_filepath>")
        print("Example: python datacleaning.py january_data.csv")
        sys.exit(1)
    
    csv_file = sys.argv[1]
    
    print(f"Loading data from: {csv_file}")
    
    try:
        # Load and clean data
        included_df, excluded_df, summary_stats = load_and_clean_data(csv_file)
        
        # Save reports
        save_reports(included_df, excluded_df, summary_stats, output_dir='./reports')
        
        print("\n✓ Data cleaning completed successfully!")
        
    except FileNotFoundError:
        print(f"Error: File '{csv_file}' not found.")
        sys.exit(1)
    except Exception as e:
        print(f"Error during data cleaning: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)