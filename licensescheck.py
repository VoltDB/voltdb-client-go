#!/usr/bin/python

import os, sys, re, shutil, subprocess

# Path to eng checkout root directory. To use this as a git pre-commit hook,
# create a symlink to this file in .git/hooks with the name pre-commit
basepath = os.path.dirname(os.path.realpath(__file__)) + os.sep
ascommithook = False

def licenseStartsHere(content, approvedLicense):
    if content.startswith(approvedLicense):
        return 1
    return 0

def verifyLicense(f, content, approvedLicense):
    if not content.startswith("/*"):
        if content.lstrip().startswith("/*"):
            print "ERROR: \"%s\" contains whitespace before initial comment." % f
            return 1
        else:
            print "ERROR: \"%s\" does not begin with a comment." % f
            return 1
    # verify license
    if licenseStartsHere(content, approvedLicense):
        return 0
    print "ERROR: \"%s\" does not start with an approved license." % f
    return 1

def verifyGofmt(f, content):
    proc = subprocess.Popen(['gofmt', f], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = proc.communicate()
    if err:
        print 'Failed executing gofmt ' + err
        sys.exit(-1)
    if out == content:
        return 0
    print "ERROR: \"%s\" fails gofmt." % f
    return 1

def readFile(filename):
    "read a file into a string"
    FH=open(filename, 'r')
    fileString = FH.read()
    FH.close()
    return fileString

def writeRepairedContent(filename, newtext, original):
    try:
        FH=open(filename + ".lcbak", 'r')
        FH.close()
    except IOError:
        FH=open(filename + ".lcbak", 'w')
        FH.write(original)
        FH.close()
    FH=open(filename, 'w')
    FH.write(newtext)
    FH.close()
    return newtext

def rmBakFile(filename):
    try:
        os.remove(filename + ".gfbak")
    except OSError:
        pass
    try:
        os.remove(filename + ".lcbak")
    except OSError:
        pass

def fixLicense(f, content, approvedLicense):
    if licenseStartsHere(content.lstrip(), approvedLicense):
        print "Fix: removing whitespace before the approved license."
        revisedcontent = content.lstrip()
    else:
        print "Fix: Inserting a default license before the original content."
        revisedcontent = approvedLicense + content
    return writeRepairedContent(f, revisedcontent,  content)

def fixGofmt(f, content):
    proc = subprocess.Popen(['gofmt', f], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = proc.communicate()
    if err:
        print 'Failed executing fixGofmt ' + err
        sys.exit(-1)
    if out != content:
        outf = f + '.gofmt'
        stdout = open(outf, "w")
        proc = subprocess.Popen(['gofmt', f], stdout=stdout, stderr=subprocess.PIPE)
        out, err = proc.communicate()
        if err:
            print 'Failed executing fixGofmt ' + err
            sys.exit(-1)
        shutil.move(f, f + '.gfbak')
        shutil.move(outf, f)

FIX_LICENSES_LEVEL = 2

def processFile(f, fix, approvedLicense):
    content = readFile(f)
    if fix:
        rmBakFile(f)
    (fixed, found) = (0, 0)

    retval = verifyLicense(f, content,  approvedLicense)
    if retval != 0:
        if fix > FIX_LICENSES_LEVEL:
            fixed += retval
            content = fixLicense(f, content, approvedLicense)
        found += retval

    retval = verifyGofmt(f, content)
    if (retval != 0):
        if fix:
            fixed += retval
            fixGofmt(f, content)
        found += retval

    return (fixed, found)

def processAllFiles(d, fix, approvedLicense):
    files = os.listdir(d)
    (fixcount, errcount) = (0, 0)
    for f in [f for f in files if not f.startswith('.')]:
        fullpath = os.path.join(d,f)
        # print fullpath
        if os.path.isdir(fullpath):
            (fixinc, errinc) = processAllFiles(fullpath, fix, approvedLicense)
        elif fullpath.endswith('.go') and not f.startswith('.'):
            (fixinc, errinc) = processFile(fullpath, fix, approvedLicense)
        else:
            (fixinc, errinc) = (0, 0)
        fixcount += fixinc
        errcount += errinc
    return (fixcount, errcount)

fix = 0
parsing_options = True

for arg in sys.argv[1:]:
    if parsing_options and arg[0:2] == "--":
        if arg == "--":
            parsing_options = False
        elif arg == "--fixws":
            fix = 1
        elif arg == "--fixall":
            fix = FIX_LICENSES_LEVEL + 1
        else:
            print 'IGNORING INVALID OPTION: "%s". It must be "--fixws" or "--fixall" or if "%s" is an additional code repo directory, it must follow a standalone "--" option.' % (arg, arg)


(fixcount, errcount) = (0, 0)
licensefile = basepath + 'tools/approved_licenses/gpl3_voltdb.txt'
(fixinc, errinc) = processAllFiles(basepath, fix, readFile(licensefile))
fixcount += fixinc
errcount += errinc

if errcount == 0:
    print "SUCCESS. Found 0 license text errors, 0 gofmt errors."
elif fix:
    print "PROGRESS? Tried to fix %d of the %d found license text or gofmt errors. Re-run licensescheck to validate. Consult .gfbak files to recover if something went wrong." % (fixcount, errcount)
else:
    print "FAILURE. Found %d license text or gofmt errors." % errcount

sys.exit(errcount)
