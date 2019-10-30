#!/usr/bin/env python3

# pylint: disable=no-self-use,missing-docstring

import unittest
import os.path
import tempfile

import numpy as np
import pandas as pd

import gql

# TODO(saito) Fix BASE_DIR if we are to run this test in Bazel.
BASE_DIR = os.path.join(os.environ['GRAIL'], 'go/src/grail.com/cmd/grail-query')

_GQL = gql.GQL(log_prefix='/tmp/gql_unittest')

class GQLTest(unittest.TestCase):
    def test_read_tidy_tsv(self) -> None:
        df = _GQL.tidytsv_to_dataframe(f'{BASE_DIR}/testdata/data2.tsv')
        print(df)
        self.assertEqual(df['B'][0], '/a')
        self.assertEqual(df['A'][0], 1)
        self.assertEqual(df['A'][1], 2)
        self.assertEqual(df['D'][1], 1000)
        self.assertEqual(df['B'][1], 's3://a')
        self.assertTrue(np.isnan(df['D'][2]))

    def test_write_tidy_tsv(self) -> None:
        df = _GQL.tidytsv_to_dataframe(f'{BASE_DIR}/testdata/data2.tsv')
        print(df)

        with tempfile.NamedTemporaryFile(suffix='.tsv') as temp_fd:
            temp_path = temp_fd.name
            _GQL.dataframe_to_tidytsv(df, temp_path)
            df2 = _GQL.tidytsv_to_dataframe(temp_path)
            print("DF2", df)
            pd.testing.assert_frame_equal(df, df2)

    def test_file_exists(self) -> None:
        s3_path = 's3://grail-ysaito/tmp/gql_test_file_exists.txt'
        with _GQL.create_file('s3://grail-ysaito/tmp/gql_test_file_exists.txt') as temp_fd:
            print("testdata", file=temp_fd)

        self.assertTrue(_GQL.file_exists(s3_path))
        self.assertFalse(_GQL.file_exists('s3://grail-ysaito/tmp/gql_test_file_exists_not.txt'))

    def test_write_tidy_tsv_s3(self) -> None:
        df = _GQL.tidytsv_to_dataframe(f'{BASE_DIR}/testdata/data2.tsv')
        print(df)

        with _GQL.create_file('s3://grail-ysaito/tmp/gql_test_write_tidy.tsv') as temp_fd:
            temp_path = temp_fd.name
            _GQL.dataframe_to_tidytsv(df, temp_path)
            df2 = _GQL.tidytsv_to_dataframe(temp_path)
            print("DF2", df)
            pd.testing.assert_frame_equal(df, df2)

    def test_eval(self) -> None:
        df = _GQL.eval(
            expr='(ccga | pick($date == 2018-04-09)).clinical | map({$patient_id, $primccat})')
        self.assertEqual([x for x in df.loc[df['patient_id'].isin(('5131', '5189'))]['primccat']],
                         ['Lung', 'Head/Neck'])

    def create_file(self) -> None:
        path = 's3://grail-ysaito/tmp/gqltesttmp.txt'
        with _GQL.create_file(path) as fd:
            print("Hello", file=fd)
        self.assertEqual(_GQL.open_file(path).read(), 'Hello\n')

if __name__ == '__main__':
    unittest.main()
