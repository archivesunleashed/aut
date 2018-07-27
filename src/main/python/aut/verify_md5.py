#!/usr/bin/env python

import argparse
import os
from subprocess import Popen, PIPE


def verify(dir_name):

    manifest_file = open(os.path.join(dir_name, "MANIFEST"), 'r')
    num_verified = 0
    num_unverified = 0
    count = 0

    for line in manifest_file.readlines():
        count += 1
        print "# " + str(count)
        file_name = line.split(' ')[0]
        md5 = line.split(' ')[1].strip()
        file_path = os.path.join(dir_name, file_name)
        # call md5sum and get stdout
        proc = Popen(['md5sum', file_path], stdout=PIPE, stderr=PIPE)
        output, error = proc.communicate()
        # verify md5
        file_md5 = output.split()[0]
        if file_md5 != md5:
            print 'wrong md5 for %s, expect %s, actual %s' % (
                file_path, md5, file_md5)
            num_unverified += 1
        else:
            print 'md5 verified for %s' % file_path
            num_verified += 1

    print "# verified: " + str(num_verified)
    print "# unverified: " + str(num_unverified)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Downloaded Collection Directory')
    parser.add_argument('dir', help='directory name')
    args = parser.parse_args()
    verify(args.dir)
