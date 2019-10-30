#!/usr/bin/env python3

"""
Build and install gql binaries and docs in S3 / Phriction.

The binaries are installed in s3://grail-bin/{linux,darwin}/amd64/grail-query.
The doc are installed in ./README.md and https://phabricator.grailbio.com/w/docs/gql/.
"""

import argparse
import concurrent.futures
import glob
import json
import logging
import os
import re
import shutil
import subprocess
import tempfile
import time
from typing import List, Dict, Tuple

GRAIL = subprocess.check_output(['git', 'rev-parse', '--show-toplevel'],
                                universal_newlines=True).strip()
GQL_DIR = os.path.join(f'{GRAIL}/go/src/grail.com/cmd/grail-query')

def call(args: List[str]) -> None:
    "Run a subprocess."
    logging.info("call: %s", args)
    subprocess.check_call(args)

def copy_s3(src: str, dest: str) -> None:
    "Copy a file to S3"
    call(['grail-file', 'cp', src, dest])

class Document:
    """A helper class for annotating a markdown document and translating it into remarkup."""

    def __init__(self, text: str) -> None:
        """
        Args:
        text: the contents of README.md file.
        """
        self.__lines = text.split('\n')
        self.__assign_section_numbers()

    def __assign_section_numbers(self) -> None:
        """Assign section numbers to each ### header line."""

        # List of (section level, section title) in the doc
        self.__sections: List[Tuple[int, str]] = []

        # Maps markdown-style section anchors (found in the original document)
        # to remarkup-style section anchors.
        self.__anchor_map: Dict[str, str] = {}

        section_numbers = [0, 0, 0, 0, 0, 0]
        for lineno in range(len(self.__lines)):
            line = self.__lines[lineno]
            m = re.match(r'^#(#+) (.*)', line)
            if not m:
                continue
            level = len(m[1]) - 1
            section_numbers[level] += 1
            for i in range(level + 1, len(section_numbers)):
                section_numbers[i] = 0
            sec = ''
            for v in section_numbers:
                if v == 0:
                    break
                sec += f'{v}.'
            old_header = m[2]
            new_header = f'{sec} {old_header}'
            self.__lines[lineno] = f'#{m[1]} {new_header}'
            self.__sections.append((len(m[1]), new_header))
            self.__anchor_map[Document.__to_markdown_hashtag(old_header)] = new_header

    @staticmethod
    def __to_remarkup_hashtag(text: str) -> str:
        """Convert section title to an implicit anchor tag using the same algorithm used
        by remarkup"""
        text = text.lower()
        text = '#' + re.sub(r'[\s\.:/]+', '-', text)
        return text[:25]  # remarkup trims anchors at 25 bytes.

    @staticmethod
    def __to_markdown_hashtag(text: str) -> str:
        """Convert section title to an implicit anchor tag using the same algorithm used
        by markdown."""
        text = text.lower()
        text = re.sub(r'[:/\(\)]', '', text)
        text = '#' + re.sub(r'[\.\s]+', '-', text)
        return text

    def __generate_remarkup_link(self, link: str) -> str:
        """Convert a markdown-style link found in the document to remarkup link"""
        if link.startswith('#'):
            return Document.__to_remarkup_hashtag(self.__anchor_map[link])
        return link

    def __generate_markdown_link(self, link: str) -> str:
        """Convert a markdown-style link found in the document to a link w/ section
        numbers."""
        if link.startswith('#'):
            return Document.__to_markdown_hashtag(self.__anchor_map[link])
        return link

    def to_markdown(self) -> str:
        """Convert the document to a prettier markdown format"""
        out: List[str] = []
        generated_toc = False
        for line in self.__lines:
            if line.startswith('##'):
                if not generated_toc:
                    # Generate TOC just before the first non-title section.
                    out.append('## Table of contents') # empty line
                    out.append('')
                    for level, title in self.__sections:
                        indent = ' ' * 2 * (level-1)
                        out.append(indent + f'* [{title}]({Document.__to_markdown_hashtag(title)})')
                    out.append('')
                    generated_toc = True
            else:
                line = re.sub(r'::(.+?)::', r'`\1`', line)
                line = re.sub(r'\[([^]]*)\]\(([^)]*)\)',
                              lambda m: f'[{m[1]}]({self.__generate_markdown_link(m[2])})', line)

            out.append(line)
        return '\n'.join(out)

    def to_remarkup(self) -> str:
        """Convert the document to Phabricator remarkup format"""
        out = ''
        prev_line = ''
        for line in self.__lines:
            line = re.sub(r'::(.+?)::', r'`\1`', line)
            # *foobar* -> //foobar//
            line = re.sub(r'\*([^*\s]+)\*', r'//\1//', line)
            # _foobar_ -> //foobar//
            line = re.sub(r'\b\_([^_\s]+)\_\b', r'//\1//', line)
            # [text](link) -> [[link|text]]
            line = re.sub(r'\[([^]]*)\]\(([^)]*)\)',
                          lambda m: f'[[{self.__generate_remarkup_link(m[2])}|{m[1]}]]', line)

            # Convert line endings. Markdown treats an empty line as a paragraph
            # delimiter, whereas remarkup treats a newline as a paragraph delimiter.
            # So here we remove the '\n', except for pre-formatted text blocks or
            # empty lines.
            if line == '':
                out += line + '\n'
                if prev_line != '':
                    out += '\n'
            elif re.match(r'\s*\|', line) or line.startswith('    ') or line.startswith('- '):
                out += line + '\n' # table or codeblock
            else:
                if prev_line != '':
                    out += ' '
                out += line
            prev_line = line
        return out



def install_doc() -> None:
    """Generate README.md and push it to the phabricator wiki page."""
    call(['bazel', 'build', '//go/src/grail.com/cmd/grail-query/generatedoc'])
    generatedoc_path = glob.glob(f'{GRAIL}/bazel-bin/go/src/grail.com/cmd/grail-query/generatedoc/*/generatedoc')[0]
    logging.info("call: %s", generatedoc_path)
    readme = Document(subprocess.check_output(
        [generatedoc_path],
        cwd=f'{GRAIL}/go/src/grail.com/cmd/grail-query',
        universal_newlines=True))

    with open(f'{GQL_DIR}/README.md', 'w') as fd:
        # Note: ToC is needed only for markdown. The Phabricator wiki server
        # automatically adds a navigation menu.
        #fd.write(generate_toc(readme))
        fd.write(readme.to_markdown())

    data = readme.to_remarkup()
    js_text = {
        "slug" : "/docs/gql",
        "title" : "GQL",
        "content": data,
    }
    log_fd = open('/tmp/arclog.txt', 'w')
    with tempfile.NamedTemporaryFile(mode='w+') as temp_fd:
        temp_fd.write(json.dumps(js_text, indent=2))
        temp_fd.seek(0)
        subprocess.check_call(['arc', 'call-conduit',
                               '--conduit-uri', 'https://phabricator.grailbio.com/',
                               'phriction.edit'],
                              stdin=temp_fd,
                              stdout=log_fd,
                              stderr=subprocess.STDOUT)

def main() -> None:
    """Main entry point."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)-15s %(message)s')
    logging.info('Workspace root is "%s"', GRAIL)
    parser = argparse.ArgumentParser()
    parser.add_argument('--skip-release',
                        action='store_true',
                        help="""Don't copy new binary in s3://grail-bin""")
    parser.add_argument('--skip-doc',
                        action='store_true',
                        help="""Don't copy doc in Phabricator wiki""")
    args = parser.parse_args()
    os.chdir(GRAIL)

    # Install binaries and docs.
    pool = concurrent.futures.ThreadPoolExecutor(max_workers=128)
    results = []
    if not args.skip_release:
        results.append(pool.submit(call, ['bazel', 'run', '//go/src/grail.com/cmd/grail-query:release']))
    if not args.skip_doc:
        results.append(pool.submit(install_doc))

    # Wait for the threads to finish.
    for result in results:
        result.result()

main()
