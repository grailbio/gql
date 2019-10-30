# python version: 3
"""
Utility types and functions for GQL.

Requires python 3.6 and above.
"""

import itertools
import logging
import os
import shutil
import subprocess
import tempfile
import threading
import time
import csv
from typing import Any, Dict, List, Optional, IO, Iterable, Tuple

import numpy as np
import pandas as pd

CACHE_DIR = os.path.join('/tmp/gql_python_cache_' + os.environ['USER'])
if not os.path.exists(CACHE_DIR):
    os.mkdir(CACHE_DIR)

def reset_cache_dir() -> None:
    """Remove all the files in CACHE_DIR."""
    shutil.rmtree(CACHE_DIR)
    os.mkdir(CACHE_DIR)

class _S3FileWriter:
    """A writeonly file-like object that copies the contents to S3 on close."""
    def __init__(self, q: 'GQL', path: str) -> None:
        self.name = path
        self.__gql = q
        self.__s3_path = path
        self.__local_path = os.path.join(CACHE_DIR, path.replace('/', '_') + 'tmp')
        self.__local_fd = open(self.__local_path, 'w')

    def __enter__(self) -> Any:
        """Implemens the context interface."""
        return self

    def __exit__(self, exc_type, excalue, traceback) -> bool:
        """Implemens the context interface."""
        if traceback is not None:
            self.__local_fd.close()
            return False
        self.close()
        return False

    def write(self, data: str) -> None:
        """Write implements the file-like-object interface."""
        self.__local_fd.write(data)

    def close(self) -> None:
        """Close implements the file-like-object interface."""
        self.__local_fd.close()
        self.__gql.copy_file(self.__local_path, self.__s3_path)

def _comment_stripper(it: Iterable[str]) -> Iterable[str]:
    "Remove CSV comment lines"
    n = 0
    for line in it:
        if line[:1] == '#':
            continue
        if not line.strip():
            continue
        n += 1
        if n == 1:
            # The first line is a comment
            continue
        yield line

class GQL:
    """This object defines methods for invoking GQL.

    Thread compatible."""

    def __init__(self,
                 log_prefix: str,
                 default_flags: List[str] = [],
                 default_params: Dict[str, str] = {},
                 default_max_retries=1) -> None:
        """The constructor compiles gql using bazel."

        Args:

        log_prefix: the pathname prefix of the log-file directory.
        For example, "/tmp/myscript". The actual log directory name will be
        <log_prefix><date>.

        default_flags: List of flags to be passed to GQL.

        default_params: List of params to be passed to GQL scripts. For example,
        {'input': 's3://mybucket/file'}.

        """

        # Build grail-query.
        temp = tempfile.NamedTemporaryFile(mode='w', delete=False)
        temp.close()
        subprocess.check_call(['bazel', 'run',
                               f'--script_path={temp.name}',
                               '--noshow_progress',
                               '--show_result=0',
                               'go/src/grail.com/cmd/grail-query:grail-query'],
                              cwd=os.environ['GRAIL'])
        self.__gql_path = temp.name

        temp = tempfile.NamedTemporaryFile(mode='w', delete=False)
        temp.close()
        subprocess.check_call(['bazel', 'run', f'--script_path={temp.name}',
                               '--noshow_progress',
                               '--show_result=0',
                               'go/src/grail.com/cmd/grail-file:grail-file'],
                              cwd=os.environ['GRAIL'])
        self.__grail_file_path = temp.name
        self.__default_flags = default_flags
        self.__default_params = default_params
        self.__default_max_retries = default_max_retries
        self.__seq = itertools.count() # for generating unique filenames
        self.__log_dir = log_prefix + time.strftime("%Y-%m-%dT%H-%M-%SZ", time.gmtime())
        if not os.path.exists(self.__log_dir):
            os.makedirs(self.__log_dir)

    def log_dir(self) -> str:
        """Return the directory that stores log files created by GQL instances"""
        return self.__log_dir

    def gql_path(self) -> str:
        """Return the absolute path of the GQL executable."""
        return self.__gql_path

    def eval(self,
             expr: str = '',
             script_path: str = '',
             label: str = '',
             flags: List[str] = [],
             gql_params: Dict[str, str] = {},
             max_retries: int = -1) -> pd.DataFrame:
        """Evaluate a GQL expression and return the result as pandas.DataFrame.
        The expression must evaluate to a gql.Table.

        Args:

        expr: the expression to evaluate.

        flags: the flags for the gql binary. If empty, the default_flags
        (constructor arg) is used.

        gql_params: the parameters to pass to the gql script. If empty, the
        default_params (constructor arg) is used.

        max_retries: max numbers of times to retry when gql fails. A negative
        value means no retry.

        Example:
          q = gql.GQL(...)
          df = q.eval('(ccga | pick($date == 2018-04-09)).clinical | map({$patient_id, $primccat})')
          print(df['patient_id'])

        """
        if (expr == '') == (script_path == ''):
            raise Exception('run: Exactly one of expr or script_path must be set')
        if label == '':
            label = 'anon_expr'
        if not flags:
            flags = self.__default_flags
        if max_retries < 0:
            max_retries = self.__default_max_retries
        if not gql_params:
            gql_params = self.__default_params

        seq = next(self.__seq)
        temp_path = f'{self.__log_dir}-{label}-expr_to_dataframe_tmp-{seq}.tsv'
        try:
            instance = _Instance(self, label=label,
                                 expr=expr,
                                 script_path=script_path,
                                 gql_flags=flags + [f'--output={temp_path}'],
                                 gql_params=gql_params,
                                 max_retries=max_retries)
            status = instance.run()
            if status != 0:
                raise Exception(f'eval {expr}: gql finished with status {status}')
            return self.tidytsv_to_dataframe(temp_path)
        finally:
            pass

    def run(self,
            expr: str = '',
            script_path: str = '',
            label: str = '',
            flags: List[str] = [],
            gql_params: Dict[str, str] = {},
            max_retries: int = 0) -> int:
        """Evaluate a GQL expression. Returns the gql process exit status.  Run is
        similar to eval, but it is used when the caller only needs the side
        effect of the evaluation.

        expr: the expression to evaluate.

        flags: the flags for the gql binary. If empty, the default_flags
        (constructor arg) is used.

        gql_params: the parameters to pass to the gql script. If empty, the
        default_params (constructor arg) is used.

        max_retries: max numbers of times to retry when gql fails. A negative
        value means no retry.

        """
        if (expr == '') == (script_path == ''):
            raise Exception('run: Exactly one of expr or script_path must be set')
        if label == '':
            label = 'anon_run'
        if not flags:
            flags = self.__default_flags
        if max_retries < 0:
            max_retries = self.__default_max_retries
        if not gql_params:
            gql_params = self.__default_params

        instance = _Instance(self,
                             label=label,
                             expr=expr,
                             script_path=script_path,
                             gql_flags=flags,
                             gql_params=gql_params,
                             max_retries=max_retries)
        return instance.run()

    def copy_tsv(self, from_path: str, to_path: str) -> int:
        """Copy a TSV-like file from from_path to to_path. Path can be a local or a S3
        file."""
        from_path = abspath(from_path)
        to_path = abspath(to_path)
        logging.info('cp %s -> %s', from_path, to_path)
        try:
            instance = _Instance(self,
                                 label='copy_tsv',
                                 script_path='',
                                 expr=f'read(`{from_path}`) | write(`{to_path}`)',
                                 gql_flags=['-overwrite-files'])
            return instance.run()
        except:
            _removeall(to_path)
            raise

    def copy_file(self, from_path: str, to_path: str):
        "Copy a file from_path to to_path as-is. Path can be a local or a S3 file."
        logging.info('cp %s -> %s', from_path, to_path)
        try:
            subprocess.check_call(
                [self.__grail_file_path, 'cp', abspath(from_path), abspath(to_path)])
        finally:
            _removeall(to_path)

    def file_exists(self, path: str) -> bool:
        """Check if the file exists. The path can refer to a local file or an S3 object."""
        try:
            unused_fd = self.open_file(path)
            unused_fd.close()
            return True
        except (subprocess.CalledProcessError, FileNotFoundError):
            # grail-file non-zero exit
            return False

    def open_file(self, path: str) -> IO[str]:
        """Open a file. The path can be either S3 or a local file. If the path refers to
        an S3 object, it is first downloaded into a temp dir.
        """

        return open(self.maybe_cache_file(path))


    def create_file(self, path: str) -> IO[str]:
        """Open a file for writing. The path can be either S3 or a local file. If the
        path refers to an S3 object, contents are first written to a local temp
        file, then copied to S3 on close.

        """

        if path.startswith('s3://'):
            return _S3FileWriter(self, path)  # type: ignore
        return open(path, 'w')

    def maybe_cache_file(self, path: str) -> str:
        """Download an S3 file to the local file system.  If path is starts with s3://,
        this function downloads the file into a temp directory and returns the temp
        path. Otherwise, it returns the path as is.

        """

        if path.startswith('s3://'):
            local_path = os.path.join(CACHE_DIR, path.replace('/', '_'))
            if not os.path.exists(local_path):
                self.copy_file(path, local_path)
            path = local_path
        return path

    def open_tsv(self, path: str) -> Iterable[List[str]]:
        """Create a CSV reader for the given path. The path can be an S3 object.  It
        also strips comment lines.

        """

        path = self.maybe_cache_file(path)
        if path.endswith('.btsv'):
            tsv_path = path.replace('.btsv', '.tsv')
            if not os.path.exists(tsv_path):
                logging.info('translate %s -> %s', path, tsv_path)
                subprocess.check_call([
                    'grail-query', '-overwrite-files', '-eval',
                    f'read(`{path}`) | write(`{tsv_path}`)'])
            path = tsv_path
        logging.info("Reading %s", path)
        return csv.reader(_comment_stripper(open(path)), delimiter='\t')

    def tidytsv_to_dataframe(self, path: str) -> pd.DataFrame:
        """Load a tidy tsv file into a pandas.DataFrame.  There must be two files: the
        tsv file itself, and the data dictionary file of form
        foo_data_dictionary.tsv, where foo is the base part of the path.

        """
        parser = _TidyTSVParser(self)
        return parser.read(path)

    def dataframe_to_tidytsv(self, df: pd.DataFrame, path: str) -> None:
        """Load a tidy tsv file into a pandas.DataFrame.  There must be two files: the
        tsv file itself, and the data dictionary file of form
        foo_data_dictionary.tsv, where foo is the base part of the path.

        """
        writer = _TidyTSVWriter(self)
        return writer.write(df, path)

def _removeall(path: str) -> None:
    if path.startswith('s3://'):
        # TODO(saito) handle this.
        return
    try:
        os.remove(path)
    except:
        pass
    try:
        shutil.rmtree(path)
    except:
        pass


def abspath(path: str) -> str:
    """Return os.path.abspath(path) if the path is on a local file system. If the
    path is of form s3:..., return the path as is.

    """
    if path.startswith('s3:'):
        return path
    return os.path.abspath(path)

class _Instance:
    """Class for running one instance of gql.

    Example:

    instance = gql._Instance(...)
    status = instance.run()
    if status != 0:
        logging.panic("job failed")
    """
    def __init__(self,
                 q: GQL,
                 label: str,
                 script_path: str = '',
                 expr: str = '',
                 gql_flags: List[str] = [],
                 gql_params: Dict[str, str] = {},
                 max_retries: int = 0) -> None:
        """
        Args:
        label: path prefix of the log files to be produced.

        script_path: the path of the *.gql file
        expr: GQL expression. Exactly one of script_path or expr must be set.

        gql_flags: flags to pass to gql
        gql_params: params to pass to gql script
        """
        if script_path == '' and expr == '':
            raise Exception('Exactly one of script_path or expr must be set')

        if label == '':
            raise Exception('label cannot be empty')
        self.log_prefix = os.path.join(q.log_dir(), label)
        if expr != '':
            prefix = self.log_prefix + "scripttmp"
            temp = tempfile.NamedTemporaryFile(mode='w', delete=False,
                                               dir=os.path.dirname(prefix),
                                               prefix=os.path.basename(prefix),
                                               suffix='.gql')
            temp.write(expr)
            temp.close()
            script_path = temp.name
            expr = ''

        self.gql = q
        self.label = label
        self.script_path = script_path
        self.gql_flags = gql_flags
        self.gql_params = gql_params
        self.max_retries = max_retries
        self.done = False
        self.done_cv = threading.Condition()
        self.main_thread: Optional[threading.Thread] = None
        self.status = 0

        self.bm_pprof_thread: Optional[threading.Thread] = None
        self.main_pprof_thread: Optional[threading.Thread] = None

    def run(self) -> int:
        """Run the GQL process. It blocks until the process finishes.
        It returns the process exit status. Zero means success.

        """

        log_path = self.log_prefix + "log.txt"
        bm_port = 2000 + hash(log_path) % 20000
        main_port = 22000 + hash(log_path) % 20000
        self.__start_pprof(bm_port, main_port)

        log_out = open(log_path, 'wb')
        gql_flags = self.gql_flags + [
            f'--pprof=:{main_port}',
            f'--bigslice.http=:{bm_port}',
            '--bigslice.parallelism=8192',
            '--bigslice.system=ec2-gql',
            #'--on-demand',
            #'--ec2machineimmortal',
        ]
        gql_params = [f'--{k}={v}' for k, v in self.gql_params]
        cmdline = [self.gql.gql_path()] + gql_flags + [self.script_path] + gql_params
        retries = 0
        while retries <= self.max_retries:
            logging.info('Starting %s >%s(retry: %s)', cmdline, log_path, retries)
            self.status = subprocess.call(cmdline, stdout=log_out, stderr=subprocess.STDOUT)
            if self.status == 0:
                break
            retries += 1
            logging.info('Error: %s finished with status %s (retry: %s)',
                         cmdline, self.status, retries)
        self.__join_pprof()
        logging.info('Command %s finished with status %s', cmdline, self.status)
        return self.status

    def __start_pprof(self, bm_port: int, main_port: int) -> None:
        """Start background threads that periodically runs pprof on the gql process."""
        self.bm_pprof_thread = threading.Thread(
            target=self.__pprof,
            args=(self.log_prefix + 'bm', f'localhost:{bm_port}/debug/bigmachine/pprof'))
        self.bm_pprof_thread.start()
        self.main_pprof_thread = threading.Thread(
            target=self.__pprof,
            args=(self.log_prefix + 'main', f'localhost:{main_port}/debug/pprof'))
        self.main_pprof_thread.start()

    def __join_pprof(self) -> None:
        """Stop and cull the pprof threads started by __start_pprof."""
        with self.done_cv:
            self.done = True
            self.done_cv.notify_all()
        assert self.bm_pprof_thread and self.main_pprof_thread
        self.bm_pprof_thread.join()
        self.main_pprof_thread.join()

    def __wait_pprof_timeout(self) -> bool:
        with self.done_cv:
            self.done_cv.wait(30)
            return not self.done

    def __pprof(self, prefix: str, uri: str) -> None:
        "A daemon thread that issues 'go tool pprof' periodically on the gql bigslice master."
        seq = 0
        log_path = self.log_prefix + 'pprof_log.txt'
        with open(log_path, 'w') as log_fd:
            while self.__wait_pprof_timeout():
                cpu_prof_path = f'{prefix}_cpu.{seq}.pprof'
                heap_prof_path = f'{prefix}_heap.{seq}.pprof'
                with open(cpu_prof_path, 'wb') as fd:
                    subprocess.call(
                        ['go', 'tool', 'pprof', '-proto',
                         f'{uri}/profile'],
                        stdout=fd, stderr=log_fd)
                with open(heap_prof_path, 'wb') as fd:
                    subprocess.call(
                        ['go', 'tool', 'pprof', '-proto',
                         f'{uri}/heap'],
                        stdout=fd, stderr=log_fd)
                time.sleep(30)
                seq += 1

def send_mail(subject: str, msg: str) -> None:
    """Send email to $USER with the given subject and message"""
    to = os.environ['USER'] + '@grailbio.com'
    p = subprocess.Popen(['ssmtp', to], stdin=subprocess.PIPE, encoding='utf-8')
    print(f"""To: {to}\r
From: {to}\r
Subject: {subject}\r
\r
{msg}\r
""", file=p.stdin)
    p.stdin.close()
    status = p.wait()
    if status != 0:
        print("Failed to send mail")

def _tidy_data_dictionary_path(tsv_path: str) -> str:
    """Return the tidy data dictionary tsv path, given the main tsv path."""
    base, ext = os.path.splitext(tsv_path)
    assert ext == ".tsv"
    return base + "_data_dictionary.tsv"

class _TidyTSVParser:
    """Helper for loading a tidy TSV file into Pandas dataframe.

    TODO(saito): Parse time and enum types. They are currently parsed as strings.
    """

    def __init__(self, q: GQL) -> None:
        self.__gql = q

    @staticmethod
    def parse_type(name: str) -> np.dtype:
        """Convert tidy data type to numpy.dtype. Unsupported data types are treated as
        just 'object's."""
        if name == 'int':
            return np.int
        elif name == 'float':
            return np.float
        else:
            return object

    @staticmethod
    def __hack_na(row: List[str], cols: List[Tuple[str, np.dtype]]) -> List[Any]:
        """Convert occurrences of "NA" in the row to something that Pandas can understand.

        Page https://pandas.pydata.org/pandas-docs/stable/missing_data.html
        discusses Pandas's support for NA in general.

        As of 2018/08, Pandas doesn't support NA for integers. Page
        https://stackoverflow.com/questions/21287624/convert-pandas-column-containing-nans-to-dtype-int
        discusses various workarouds.

        """

        parsed: List[Any] = []
        for i, v in enumerate(row):
            if v != 'NA':
                parsed.append(v)
                continue
            if cols[i][1] == np.float:
                parsed.append(np.nan)
            elif cols[i][1] == np.int:
                parsed.append(-9999999)
            else:
                parsed.append(None)
        return parsed

    def __read_dictionary(self, tsv_path: str) -> List[Tuple[str, np.dtype]]:
        """Parse a tidy dictionary TSV file into a list of (column name, numpy
        type)s."""
        cols: List[Tuple[str, np.dtype]] = []
        n = 0
        for row in self.__gql.open_tsv(_tidy_data_dictionary_path(tsv_path)):
            n += 1
            #if n == 1:  # 1st row list columns.
            #    continue
            cols.append((row[0], _TidyTSVParser.parse_type(row[1])))
        return cols

    def read(self, tsv_path: str) -> pd.DataFrame:
        """Read a tidy TSV file into a new dataframe."""
        cols = self.__read_dictionary(tsv_path)
        rows = [_TidyTSVParser.__hack_na(row, cols) for row in self.__gql.open_tsv(tsv_path)]
        df = pd.DataFrame(rows, columns=[col[0] for col in cols])
        df = df.astype({col_name:col_type for col_name, col_type in cols})
        return df

class _TidyTSVWriter:
    """Helper for writing Pandas dataframeloading as a tidy TSV file.

    TODO(saito): Parse time and enum types. They are currently parsed as strings.
    """

    def __init__(self, q: GQL) -> None:
        self.__gql = q

    @staticmethod
    def numpy_type_to_tidy(t: np.dtype) -> str:
        """Convert tidy data type to numpy.dtype. Unsupported data types are treated as
        just 'object's."""
        if t == np.int:
            return 'int'
        if t == np.float:
            return 'float'
        return 'string'

    @staticmethod
    def numpy_value_to_tidy(v: Any) -> str:
        """Convert a numby scalar value to a tidy column value."""
        if v in (None, -9999999) or (isinstance(v, float) and np.isnan(v)):
            return "NA"
        return f'{v}'

    def write(self, df: pd.DataFrame, tsv_path: str) -> None:
        """Write a dataframe to a tidy TSV file."""

        with self.__gql.create_file(_tidy_data_dictionary_path(tsv_path)) as fd:
            print('column_name\ttype\tdescription', file=fd)
            for col_name, col_type in zip(df.columns, df.dtypes):
                tidy_type = _TidyTSVWriter.numpy_type_to_tidy(col_type)
                print(f'{col_name}\t{tidy_type}\tUnknown', file=fd)

        with self.__gql.create_file(tsv_path) as fd:
            print('\t'.join(df.columns), file=fd)
            for _, np_row in df.iterrows():
                tidy_row = [_TidyTSVWriter.numpy_value_to_tidy(np_col) for np_col in np_row]
                print('\t'.join(tidy_row), file=fd)
