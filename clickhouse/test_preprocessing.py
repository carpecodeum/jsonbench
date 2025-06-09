#!/usr/bin/env python3
"""
Test suite for the JSON to variants preprocessing script.
Validates data transformation accuracy and handles edge cases.
"""

import json
import tempfile
import os
import sys
import gzip
from io import StringIO
import unittest
from datetime import datetime

# Add the current directory to Python path to import the preprocessing script
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Import the preprocessing functions
from preprocess_json_to_variants import extract_fields, escape_tsv_value, process_file

class TestPreprocessing(unittest.TestCase):
    """Test suite for JSON preprocessing functions."""

    def setUp(self):
        """Set up test data for each test."""
        # Sample Bluesky JSON records
        self.sample_commit = {
            "did": "did:plc:example123",
            "time_us": 1700567149000167,
            "kind": "commit",
            "commit": {
                "rev": "3jui7t2rxnk2a",
                "operation": "create",
                "collection": "app.bsky.feed.post",
                "rkey": "3jui7t2rxnk2c",
                "cid": "bafyreihg6qgx4x7w4q"
            },
            "record": {
                "$type": "app.bsky.feed.post",
                "text": "Hello World!",
                "createdAt": "2024-11-21T11:25:49.001Z"
            }
        }
        
        self.sample_identity = {
            "did": "did:plc:example456",
            "time_us": 1700567150000000,
            "kind": "identity",
            "identity": {
                "handle": "user.example.com",
                "seq": 12345
            }
        }
        
        self.sample_account = {
            "did": "did:plc:example789",
            "time_us": 1700567151000000,
            "kind": "account",
            "account": {
                "active": True,
                "status": "valid"
            }
        }

    def test_extract_fields_commit(self):
        """Test extracting fields from valid commit data."""
        result = extract_fields(self.sample_commit)
        
        # Should return a tuple of extracted values
        self.assertIsInstance(result, tuple)
        self.assertEqual(len(result), 11)  # Expected number of fields
        
        # Check specific values
        self.assertEqual(result[0], 'did:plc:example123')  # did
        self.assertEqual(result[1], 1700567149000167)      # time_us
        self.assertEqual(result[2], 'commit')              # kind
        self.assertEqual(result[4], '3jui7t2rxnk2a')      # commit_rev
        self.assertEqual(result[5], 'create')             # commit_operation
        self.assertEqual(result[6], 'app.bsky.feed.post') # commit_collection

    def test_extract_fields_identity(self):
        """Test extracting fields from identity record."""
        result = extract_fields(self.sample_identity)
        
        self.assertIsInstance(result, tuple)
        self.assertEqual(len(result), 11)
        
        # Check that commit fields are empty for identity records
        self.assertEqual(result[0], 'did:plc:example456')  # did
        self.assertEqual(result[2], 'identity')            # kind
        self.assertEqual(result[4], '')                    # commit_rev should be empty
        self.assertEqual(result[5], '')                    # commit_operation should be empty
        self.assertEqual(result[6], '')                    # commit_collection should be empty

    def test_extract_fields_partial_commit(self):
        """Test extracting fields when some commit fields are missing."""
        partial_commit = {
            "did": "did:plc:partial",
            "time_us": 1700567149000000,
            "kind": "commit",
            "commit": {
                "operation": "delete",
                "collection": "app.bsky.feed.like"
                # Missing rev, rkey, cid
            }
        }
        
        result = extract_fields(partial_commit)
        
        self.assertEqual(result[0], 'did:plc:partial')     # did
        self.assertEqual(result[2], 'commit')              # kind
        self.assertEqual(result[4], '')                    # commit_rev should be empty
        self.assertEqual(result[5], 'delete')              # commit_operation
        self.assertEqual(result[6], 'app.bsky.feed.like')  # commit_collection

    def test_extract_fields_missing_fields(self):
        """Test extracting fields when required fields are missing."""
        minimal_record = {"some": "data"}
        result = extract_fields(minimal_record)
        
        # Should still return a tuple with default values
        self.assertIsInstance(result, tuple)
        self.assertEqual(len(result), 11)
        
        # Check that missing fields get default values
        self.assertEqual(result[0], '')  # did should be empty
        self.assertEqual(result[1], 0)   # time_us should be 0
        self.assertEqual(result[2], '')  # kind should be empty

    def test_escape_tsv_value_string(self):
        """Test TSV escaping for string values."""
        # Test string with tabs and newlines
        test_string = "Hello\tWorld\nWith\rSpecial chars"
        result = escape_tsv_value(test_string)
        
        # Should escape tabs and newlines
        self.assertNotIn('\t', result)
        self.assertNotIn('\n', result)
        self.assertNotIn('\r', result)

    def test_escape_tsv_value_number(self):
        """Test TSV escaping for numeric values."""
        result = escape_tsv_value(12345)
        self.assertEqual(result, '12345')

    def test_escape_tsv_value_none(self):
        """Test TSV escaping for None values."""
        result = escape_tsv_value(None)
        self.assertEqual(result, '')

    def test_escape_tsv_value_json(self):
        """Test TSV escaping for JSON objects."""
        test_json = {"key": "value\twith\ttabs"}
        result = escape_tsv_value(test_json)
        
        # Should be JSON string without tabs/newlines
        self.assertIsInstance(result, str)
        self.assertNotIn('\t', result)
        self.assertNotIn('\n', result)

class TestPreprocessingIntegration(unittest.TestCase):
    """Integration tests for the complete preprocessing workflow."""

    def setUp(self):
        """Set up test files."""
        self.test_data = [
            {
                "did": "did:plc:test1",
                "time_us": 1700567149000167,
                "kind": "commit",
                "commit": {
                    "rev": "rev1",
                    "operation": "create",
                    "collection": "app.bsky.feed.post",
                    "rkey": "rkey1",
                    "cid": "cid1"
                },
                "record": {
                    "$type": "app.bsky.feed.post",
                    "text": "Test post"
                }
            },
            {
                "did": "did:plc:test2",
                "time_us": 1700567150000000,
                "kind": "identity",
                "identity": {
                    "handle": "test.user"
                }
            }
        ]

    def test_full_preprocessing_workflow(self):
        """Test the complete preprocessing workflow with sample data."""
        # Create a temporary input file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as temp_input:
            for record in self.test_data:
                temp_input.write(json.dumps(record) + '\n')
            temp_input_path = temp_input.name

        # Create a temporary output file path
        temp_output_path = temp_input_path.replace('.json', '_preprocessed.tsv')

        try:
            # Run preprocessing directly
            process_file(temp_input_path, temp_output_path, max_records=None)

            # Verify output file was created
            self.assertTrue(os.path.exists(temp_output_path))

            # Verify output content
            with open(temp_output_path, 'r') as f:
                lines = f.readlines()

            # Should have header + 2 data lines
            self.assertEqual(len(lines), 3)

            # Check header
            header = lines[0].strip().split('\t')
            expected_columns = [
                'did', 'time_us', 'kind', 'timestamp_col',
                'commit_rev', 'commit_operation', 'commit_collection',
                'commit_rkey', 'commit_cid', 'record_type', 'original_json'
            ]
            self.assertEqual(header, expected_columns)

            # Check first data row (commit)
            row1 = lines[1].strip().split('\t')
            self.assertEqual(row1[0], 'did:plc:test1')
            self.assertEqual(row1[2], 'commit')
            self.assertEqual(row1[5], 'create')
            self.assertEqual(row1[6], 'app.bsky.feed.post')

            # Check second data row (identity)
            row2 = lines[2].strip().split('\t')
            self.assertEqual(row2[0], 'did:plc:test2')
            self.assertEqual(row2[2], 'identity')
            self.assertEqual(row2[5], '')  # commit_operation should be empty

        finally:
            # Clean up temporary files
            if os.path.exists(temp_input_path):
                os.unlink(temp_input_path)
            if os.path.exists(temp_output_path):
                os.unlink(temp_output_path)

    def test_gzipped_input_processing(self):
        """Test preprocessing with gzipped input file."""
        # Create a temporary gzipped input file
        with tempfile.NamedTemporaryFile(suffix='.json.gz', delete=False) as temp_file:
            temp_input_path = temp_file.name

        with gzip.open(temp_input_path, 'wt') as gz_file:
            for record in self.test_data:
                gz_file.write(json.dumps(record) + '\n')

        temp_output_path = temp_input_path.replace('.json.gz', '_preprocessed.tsv')

        try:
            # Run preprocessing
            process_file(temp_input_path, temp_output_path, max_records=None)

            # Verify output was created and has correct content
            self.assertTrue(os.path.exists(temp_output_path))
            
            with open(temp_output_path, 'r') as f:
                lines = f.readlines()
            
            # Should have header + 2 data lines
            self.assertEqual(len(lines), 3)

        finally:
            # Clean up
            if os.path.exists(temp_input_path):
                os.unlink(temp_input_path)
            if os.path.exists(temp_output_path):
                os.unlink(temp_output_path)

    def test_limited_record_processing(self):
        """Test preprocessing with max_records limit."""
        # Create test file with 5 records
        test_records = []
        for i in range(5):
            record = {
                "did": f"did:plc:test{i}",
                "time_us": 1700567149000167 + i,
                "kind": "commit"
            }
            test_records.append(record)

        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as temp_input:
            for record in test_records:
                temp_input.write(json.dumps(record) + '\n')
            temp_input_path = temp_input.name

        temp_output_path = temp_input_path.replace('.json', '_preprocessed.tsv')

        try:
            # Process only 3 records
            process_file(temp_input_path, temp_output_path, max_records=3)

            with open(temp_output_path, 'r') as f:
                lines = f.readlines()
            
            # Should have header + 3 data lines (limited)
            self.assertEqual(len(lines), 4)

        finally:
            if os.path.exists(temp_input_path):
                os.unlink(temp_input_path)
            if os.path.exists(temp_output_path):
                os.unlink(temp_output_path)

def run_performance_test():
    """Quick performance test to ensure preprocessing speed is reasonable."""
    print("\n=== Performance Test ===")
    
    # Generate larger test dataset
    test_records = []
    for i in range(1000):
        record = {
            "did": f"did:plc:test{i}",
            "time_us": 1700567149000167 + i,
            "kind": "commit" if i % 2 == 0 else "identity",
            "commit": {
                "rev": f"rev{i}",
                "operation": "create",
                "collection": "app.bsky.feed.post",
                "rkey": f"rkey{i}",
                "cid": f"cid{i}"
            } if i % 2 == 0 else None
        }
        test_records.append(record)

    # Create temporary files
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as temp_input:
        for record in test_records:
            temp_input.write(json.dumps(record) + '\n')
        temp_input_path = temp_input.name

    temp_output_path = temp_input_path.replace('.json', '_preprocessed.tsv')

    try:
        import time
        
        start_time = time.time()
        
        # Run preprocessing
        process_file(temp_input_path, temp_output_path, max_records=None)
        
        end_time = time.time()
        processing_time = end_time - start_time
        
        # Verify output
        with open(temp_output_path, 'r') as f:
            output_lines = len(f.readlines())
        
        print(f"Processed 1000 records in {processing_time:.3f} seconds")
        print(f"Rate: {1000/processing_time:.0f} records/second")
        print(f"Output lines: {output_lines} (expected: 1001 including header)")
        
        # Performance assertion - should process at least 100 records/second
        assert 1000/processing_time > 100, f"Processing too slow: {1000/processing_time:.0f} records/second"
        assert output_lines == 1001, f"Incorrect output line count: {output_lines}"
        
        print("✓ Performance test passed")

    finally:
        if os.path.exists(temp_input_path):
            os.unlink(temp_input_path)
        if os.path.exists(temp_output_path):
            os.unlink(temp_output_path)

def run_data_validation_test():
    """Test with real-world sample data to validate field extraction."""
    print("\n=== Data Validation Test ===")
    
    # Real-world samples from Bluesky
    real_samples = [
        {
            "did": "did:plc:yj3sjq3blzpynh27cumnp5ks",
            "time_us": 1732192111000167,
            "kind": "commit",
            "commit": {
                "rev": "3lbqfktkfhc2a",
                "operation": "create",
                "collection": "app.bsky.feed.like",
                "rkey": "3lbqfktkhk42e",
                "cid": "bafyreicjufzxwzkhbm4bxtx7zy5dppgggomllnyefv7h5eekyitqhvkgty"
            },
            "record": {
                "$type": "app.bsky.feed.like",
                "subject": {
                    "uri": "at://did:plc:l5o3qjrmfztir54cpwlv2eme/app.bsky.feed.post/3lbqfkr62tu2o",
                    "cid": "bafyreifuabtxlygtnlcqf5sgk3snr3lbqhstb3mgzslkckxdpczklguxsa"
                },
                "createdAt": "2024-11-21T11:41:51.001Z"
            }
        },
        {
            "did": "did:plc:l5o3qjrmfztir54cpwlv2eme",
            "time_us": 1732192111001905,
            "kind": "commit",
            "commit": {
                "rev": "3lbqfkr62tu2o",
                "operation": "create",
                "collection": "app.bsky.feed.post",
                "rkey": "3lbqfkr62tu2o",
                "cid": "bafyreifuabtxlygtnlcqf5sgk3snr3lbqhstb3mgzslkckxdpczklguxsa"
            },
            "record": {
                "$type": "app.bsky.feed.post",
                "text": "Bluesky growth is nuts",
                "createdAt": "2024-11-21T11:41:51.002Z"
            }
        }
    ]
    
    # Test field extraction
    for i, sample in enumerate(real_samples):
        print(f"Testing sample {i+1}:")
        result = extract_fields(sample)
        
        print(f"  DID: {result[0]}")
        print(f"  Kind: {result[2]}")
        print(f"  Collection: {result[6]}")
        print(f"  Operation: {result[5]}")
        print(f"  Record type: {result[9]}")
        
        # Basic validations
        assert result[0] == sample['did'], f"DID mismatch"
        assert result[2] == sample['kind'], f"Kind mismatch"
        
        if sample.get('commit'):
            assert result[6] == sample['commit']['collection'], f"Collection mismatch"
            assert result[5] == sample['commit']['operation'], f"Operation mismatch"
        
        print(f"  ✓ Sample {i+1} validated")
    
    print("✓ Data validation test passed")

if __name__ == '__main__':
    print("Running JSON preprocessing tests...")
    
    # Run unit tests
    unittest.main(argv=[''], exit=False, verbosity=2)
    
    # Run performance test
    run_performance_test()
    
    # Run data validation test
    run_data_validation_test()
    
    print("\n=== All tests completed ===") 