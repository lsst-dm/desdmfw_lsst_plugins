#!/usr/bin/env python
"""
Specialized wrapper to run LSST command line tasks 
"""

import os
import string
import sys
import re
import tarfile
import argparse
import shutil
import yaml

from despymisc import miscutils
from intgutils import intgmisc
from intgutils import basic_wrapper
from intgutils import intgdefs
from intgutils import queryutils
from intgutils import wcl
import intgutils.replace_funcs as repfunc


class GenWrapLSST(basic_wrapper.BasicWrapper):
    """ Class to run LSST command line tasks """

    def __init__(self, wclfile, debug=1):
        basic_wrapper.BasicWrapper.__init__(self, wclfile, debug)

        if 'wrapper' in self.inputwcl:
            # Specialized: initialize repo directory if doesn't exist
            if 'job_repo_dir' in self.inputwcl['wrapper'] and 'mapper' in self.inputwcl['wrapper']:
                jrdir = repfunc.replace_vars_single(self.inputwcl['wrapper']['job_repo_dir'], self.inputwcl,
                                                   {intgdefs.REPLACE_VARS: True,
                                                   'expand': True, 'keepvars': False})
                which_mapper = repfunc.replace_vars_single(self.inputwcl['wrapper']['mapper'], self.inputwcl,
                                                   {intgdefs.REPLACE_VARS: True,
                                                   'expand': True, 'keepvars': False})

                #if not os.path.exists(self.inputwcl['wrapper']['job_repo_dir']):
                if not os.path.exists(jrdir):
                    miscutils.coremakedirs(jrdir)

                #MMG if 'butler_template' in self.inputwcl['wrapper'] and not os.path.exists(os.path.join(jrdir, 'repositoryCfg.yaml')):
                if 'butler_template' in self.inputwcl['wrapper']:
                    btwcl = wcl.WCL()
                    btfile = repfunc.replace_vars_single(self.inputwcl['wrapper']['butler_template'], self.inputwcl,
                                                         {intgdefs.REPLACE_VARS: True,
                                                         'expand': True, 'keepvars': False})

                    # read yaml file with directory/filename templates
                    policy = {}
                    with open(btfile) as infh:
                        policy = yaml.load(infh)

                    # replace framework variables like reqnum in patterns
                    for wkey in policy:
                        for dtype in policy[wkey]:
                            template_str = repfunc.replace_vars_single(policy[wkey][dtype], self.inputwcl,
                                                                       {intgdefs.REPLACE_VARS: True,
                                                                       'expand': True, 'keepvars': False})
                            policy[wkey][dtype] = {'template': str(template_str)}

                    # the following should make a yaml config file for the Butler
                    # must set root to empty directory for this to work
                    from lsst.daf.persistence import Butler
                    mapper_instance = miscutils.dynamically_load_class(which_mapper)
                    b = Butler(outputs={'root': 'tmprepo', 
                               'mapper': mapper_instance, 
                               'policy': policy}
                              )
                    os.rename('tmprepo/repositoryCfg.yaml', os.path.join(jrdir, 'repositoryCfg.yaml'))
                    os.rmdir('tmprepo')
                else:
                    mapperfile = os.path.join(jrdir, '_mapper')
                    if not os.path.exists(mapperfile):
                        with open(mapperfile, 'w') as mapfh:
                            mapfh.write(which_mapper)
                        

                if 'ref_cats_root' in self.inputwcl['wrapper']:
                    rcroot = repfunc.replace_vars_single(self.inputwcl['wrapper']['ref_cats_root'], 
                                                         self.inputwcl,
                                                         {intgdefs.REPLACE_VARS: True,
                                                         'expand': True, 'keepvars': False})
                    if not os.path.exists(rcroot):
                        raise IOError('ref_cats_root (%s) does not exist' % rcroot)

                    jrrc = os.path.join(jrdir, 'ref_cats')
                    if not os.path.exists(jrrc):
                        os.symlink(rcroot, jrrc)

            # Specialized: untar files (e.g., reference catalog)
            # untar_files is comma-separated list of tarballs 
            #     (should be references to file entries)
            # Code will untar tarball in same path as tarball 
            # Code does not modify any wcl (i.e., no input def or output def changes)
            if 'untar_files' in self.inputwcl['wrapper']:
                tballs, _ = repfunc.replace_vars(self.inputwcl['wrapper']['untar_files'],
                                              self.inputwcl,
                                              {intgdefs.REPLACE_VARS: True,
                                               'expand': True, 'keepvars': False})
                

                
                if isinstance(tballs, str):
                    tballs = [tballs]
                for tar_filename in tballs:
                    miscutils.fwdebug_print("INFO: tar_filename %s " % (tar_filename),
                                            basic_wrapper.WRAPPER_OUTPUT_PREFIX)
                    tardir = os.path.dirname(tar_filename)
                    if tar_filename.endswith('.gz'):
                        mode = 'r:gz'
                    else:
                        mode = 'r'
                    with tarfile.open(tar_filename, mode) as tar:
                        tar.extractall(tardir)




    def transform_inputs(self, exwcl):
        """ Method to prepare the inputs """
        # ingest inputs into Butler repository
        
        self.start_exec_task('transform_inputs')

        # HACK assuming single exec section because otherwise need exkey
        #      to pass into get_fullnames
        ins, _ = intgmisc.get_fullnames(self.inputwcl, self.inputwcl)
        for sect in ins:
            sectkeys = sect.lower().split('.')

            if sectkeys[0] == intgdefs.IW_LIST_SECT:
                #filesect_name = sectkeys[2]
                continue
            else:
                filesect_name = sectkeys[1] 

            filesect = self.inputwcl[intgdefs.IW_FILE_SECT][filesect_name]

            # Specialized: rename input files  (rename files prior to Butler repo ingestion)
            if 'rename_file' in filesect:
                # dst should not have path as code assumes same path as src
                # src should be single file (hence the previous check for being in file sect)
                src = repfunc.replace_vars_single(ins[sect].pop(), self.inputwcl,
                                                  {intgdefs.REPLACE_VARS: True,
                                                  'expand': True, 'keepvars': False})
                dest = repfunc.replace_vars_single(filesect['rename_file'], self.inputwcl,
                                                   {intgdefs.REPLACE_VARS: True,
                                                   'expand': True, 'keepvars': False})
                #if isinstance(val, list):
                #    raise ValueError('rename_files expanded into multiple src files which is currently not supported (%s)' % src)

                srcdir = os.path.dirname(src)
                    
                miscutils.fwdebug_print("INFO: rename %s to %s " % (src, os.path.join(srcdir, dest)),
                                        basic_wrapper.WRAPPER_OUTPUT_PREFIX)
                shutil.copyfile(src, os.path.join(srcdir, dest))

            # if need to ingest input files into butler repository
            # not all inputs are ingested (e.g., ref cats, bf kernel, etc)
            if 'repoingest' in filesect:
                # create base repo ingest command line (minus actual filename)
                basecmd = repfunc.replace_vars_single(filesect['repoingest'], self.inputwcl,
                                                      {intgdefs.REPLACE_VARS: True,
                                                      'expand': True, 'keepvars': False})

                for fname in ins[sect]:
                    # create final repo ingest command line replacing xxxfilenamexxx 
                    #   with the filename
                    repocmd = re.sub("xxxfilenamexxx", fname, basecmd)
                    miscutils.fwdebug_print("INFO: repocmd = %s" % repocmd,
                                            basic_wrapper.WRAPPER_OUTPUT_PREFIX)
                
                    # run repo ingest command collecting wait4 process info
                    #     in case we want to modify code to do something with it later
                    (retcode, procinfo) = intgmisc.run_exec(repocmd)
                    #if retcode != 0:
                    #    raise RuntimeError('Problem ingesting file into butler repo (%s)' % repocmd)

        self.end_exec_task(0)

    ######################################################################  
    #def transform_outputs(self, exwcl):
    #    """ Method to modify outputs prior to ingestion """
    #    
    #    self.start_exec_task('transform_outputs')
#
#        # HACK assuming single exec section because otherwise need exkey
#        #      to pass into get_fullnames
#        _, outs = intgmisc.get_fullnames(self.inputwcl, self.inputwcl)
#        for sect in outs:
#            print sect
#            print outs.keys();
#
#            # if need to ingest input files into butler repository
#            # not all inputs are ingested (e.g., ref cats, bf kernel, etc)
#            fsect = self.inputwcl.get(sect)
#            if sect.startswith(intgdefs.IW_FILE_SECT):  
#                for fname in outs[sect]:
#                    # create final repo ingest command line replacing xxxfilenamexxx 
#                    #   with the filename
#                    repocmd = re.sub("xxxfilenamexxx", fname, basecmd)
#                    miscutils.fwdebug_print("INFO: repocmd = %s" % repocmd,
#                                            basic_wrapper.WRAPPER_OUTPUT_PREFIX)
#                    
#                if 'rename_file' in fsect:
#                    # dst should not have path as code assumes same path as src
#                    # src should be single file (hence the previous check for being in file sect)
#                    print outs[sect]
#                    print fsect['rename_file']
#                    src = repfunc.replace_vars_single(outs[sect].pop(), self.inputwcl,
#                                                      {intgdefs.REPLACE_VARS: True,
#                                                      'expand': True, 'keepvars': False})
#                    dest = repfunc.replace_vars_single(fsect['rename_file'], self.inputwcl,
#                                                       {intgdefs.REPLACE_VARS: True,
#                                                       'expand': True, 'keepvars': False})
#                    #if isinstance(val, list):
#                    #    raise ValueError('rename_files expanded into multiple src files which is currently not supported (%s)' % src)
#
#                    srcdir = os.path.dirname(src)
#                    
#                    miscutils.fwdebug_print("INFO: rename %s to %s " % (src, os.path.join(srcdir, dest)),
#                                            basic_wrapper.WRAPPER_OUTPUT_PREFIX)
#                    shutil.copyfile(src, os.path.join(srcdir, dest))
#
#
#        self.end_exec_task(0)
#

    def create_command_line(self, execnum, exwcl):
        if miscutils.fwdebug_check(3, 'GENWRAP_LSST_DEBUG'):
            miscutils.fwdebug_print("execnum = '%s', exwcl = '%s'" % (execnum, exwcl),
                                    basic_wrapper.WRAPPER_OUTPUT_PREFIX)
        self.start_exec_task('create_command_line')

        basic_wrapper.BasicWrapper.create_command_line(self, execnum, exwcl)

        if miscutils.fwdebug_check(3, 'GENWRAP_LSST_DEBUG'):
            miscutils.fwdebug_print("pre cmdline changes: = '%s'" % self.curr_exec['cmdline'], basic_wrapper.WRAPPER_OUTPUT_PREFIX)

        if 'wrapper' in self.inputwcl:
            # list.corr.img_corr:--selectId visit=${visit} ccd=${ccd}
            if 'per_file_cmdline' in self.inputwcl['wrapper']:
                tot_add_cmd = ''
                
                (whichfiles, cmd_add_pat) = self.inputwcl['wrapper']['per_file_cmdline'].split(':')
                sectkeys = whichfiles.split('.')
                if sectkeys[0] == intgdefs.IW_FILE_SECT:    
                    raise NotImplementedError('Cannot do per_file_cmdline on file sect') 

                elif sectkeys[0] == intgdefs.IW_LIST_SECT:
                    (_, listsect, filesect) = sectkeys

                    ldict = self.inputwcl[intgdefs.IW_LIST_SECT][listsect]

                    # check list itself exists
                    listname = ldict['fullname']
                    if miscutils.fwdebug_check(3, 'GENWRAP_LSST_DEBUG'):
                        miscutils.fwdebug_print("\tINFO: Checking existence of '%s'" % listname,
                                                basic_wrapper.WRAPPER_OUTPUT_PREFIX)

                    if not os.path.exists(listname):
                        miscutils.fwdebug_print("\tError: list '%s' does not exist." % listname,
                                                basic_wrapper.WRAPPER_OUTPUT_PREFIX)
                        raise IOError("List not found: %s does not exist" % listname)

                    # get list format: space separated, csv, wcl, etc
                    listfmt = intgdefs.DEFAULT_LIST_FORMAT
                    if intgdefs.LIST_FORMAT in ldict:
                        listfmt = ldict[intgdefs.LIST_FORMAT]

                    # read list file which needs to have information needed in per_file_cmdline 
                    listwcl = self.read_listfile(listname, listfmt, ldict['columns'])
    
                    # string with normal FW vars so can use normal replace funcs
                    cmd_base_pat = self._change_vars_parens(cmd_add_pat)

                    # for each file (specifically: for each line, for each file)
                    for wlname, wldict in listwcl['list']['line'].items():
                        searchobj = None
                        if filesect in wldict['file']:
                            searchobj = wldict['file'][filesect] 
                        elif len(wldict['file']) == 1:
                            searchobj = wldict['file'].values()[0]
                        else:
                            raise ValueError('Cannot find file %s in put list (%s)' % (filesect, whichfiles))
                        add_cmd_str = repfunc.replace_vars_single(cmd_base_pat, self.inputwcl,
                                                                  {'searchobj': searchobj,
                                                                   intgdefs.REPLACE_VARS: True,
                                                                   'expand': True, 'keepvars': False})
                        tot_add_cmd += ' ' + add_cmd_str
                    
                self.curr_exec['cmdline'] += tot_add_cmd
            elif 'add_cmdline' in self.inputwcl['wrapper']:
                add_cmdline = repfunc.replace_vars_single(self.inputwcl['wrapper']['add_cmdline'], 
                                                          self.inputwcl,
                                                          { intgdefs.REPLACE_VARS: True,
                                                           'expand': True, 'keepvars': False}) 
                if miscutils.fwdebug_check(3, 'GENWRAP_LSST_DEBUG'):
                    miscutils.fwdebug_print("\tINFO: add_cmdline = %s" % (add_cmdline),
                                            basic_wrapper.WRAPPER_OUTPUT_PREFIX)
                
                m = re.match(r"'(\S+)'.join\(([^)]+)\)", add_cmdline)
                if m:
                    joinstr = m.group(1)
                    what_vals_to_join = m.group(2)
    
                    sectkeys = what_vals_to_join.split('.')

                    if sectkeys[0] == intgdefs.IW_FILE_SECT:    
                        raise NotImplementedError('Cannot do add_cmdline on file sect') 
                    elif sectkeys[0] == intgdefs.IW_LIST_SECT:
                        (_, listsect, filesect, fileval) = sectkeys
                        if miscutils.fwdebug_check(3, 'GENWRAP_LSST_DEBUG'):
                            miscutils.fwdebug_print("\tINFO: list %s file %s value %s" % (listsect, filesect, fileval),
                                                    basic_wrapper.WRAPPER_OUTPUT_PREFIX)

                        ldict = self.inputwcl[intgdefs.IW_LIST_SECT][listsect]

                        # check list itself exists
                        listname = ldict['fullname']
                        if miscutils.fwdebug_check(3, 'GENWRAP_LSST_DEBUG'):
                            miscutils.fwdebug_print("\tINFO: Checking existence of '%s'" % listname,
                                                    basic_wrapper.WRAPPER_OUTPUT_PREFIX)

                        if not os.path.exists(listname):
                            miscutils.fwdebug_print("\tError: list '%s' does not exist." % listname,
                                                    basic_wrapper.WRAPPER_OUTPUT_PREFIX)
                            raise IOError("List not found: %s does not exist" % listname)

                        # get list format: space separated, csv, wcl, etc
                        listfmt = intgdefs.DEFAULT_LIST_FORMAT
                        if intgdefs.LIST_FORMAT in ldict:
                            listfmt = ldict[intgdefs.LIST_FORMAT]

                        # read list file which needs to have information needed in per_file_cmdline 
                        listwcl = self.read_listfile(listname, listfmt, ldict['columns'])

                        joinvals = set()
                        # for each file (specifically: for each line, for each file)
                        for wlname, wldict in listwcl['list']['line'].items():
                            searchobj = None
                            if filesect in wldict['file']:
                                searchobj = wldict['file'][filesect] 
                            elif len(wldict['file']) == 1:
                                searchobj = wldict['file'].values()[0]
                            else:
                                raise ValueError('Cannot find file %s in put list (%s)' % (filesect, whichfiles))

                            if fileval in searchobj:
                                joinvals.add(searchobj[fileval])
                            else:
                                raise ValueError('Cannot find value %s from file %s' % (fileval, searchobj['filename']))

                        if miscutils.fwdebug_check(3, 'GENWRAP_LSST_DEBUG'):
                            miscutils.fwdebug_print("\tINFO: joinvals = %s" % (joinvals),
                                                    basic_wrapper.WRAPPER_OUTPUT_PREFIX)

                        newcmd = joinstr.join(list(joinvals))
                        self.curr_exec['cmdline'] += newcmd
                    else:
                        raise ValueError('Invalid section name (%s) ' % sectkeys[0])
                else:
                    raise NotImplementedError('Trying to use add_cmdline feature not implemented yet (%s)' % add_cmdline) 
                    

        self.end_exec_task(0)

    def run_exec(self):
        basic_wrapper.BasicWrapper.run_exec(self)
        # database table currently holds 4000 characters.   
        # long term discussion about how to handle this in future
        self.curr_exec['cmdline'] = self.curr_exec['cmdline'][:3995]


##########
    @classmethod
    def _change_vars_parens(cls, str1):
        table = string.maketrans('()', '{}')
        return str1.translate(table)



    def read_listfile(self, listfile, linefmt, colstr):
        """ Read a list file into a std nested dict """

        if miscutils.fwdebug_check(3, 'GENWRAP_LSST_DEBUG'):
            miscutils.fwdebug_print('colstr=%s' % colstr)

        columns = intgmisc.convert_col_string_to_list(colstr, False)

        if miscutils.fwdebug_check(3, 'GENWRAP_LSST_DEBUG'):
            miscutils.fwdebug_print('columns=%s' % columns)

        mywcl = None
        if linefmt == 'config' or linefmt == 'wcl':
            mywcl = wcl.WCL()
            mywcl.read(listfile)
        else:
            mylist = []
            with open(listfile, 'r') as listfh:
                for line in listfh:
                    line = line.strip()

                    # convert line into python list
                    lineinfo = []
                    if linefmt == 'textcsv':
                        lineinfo = miscutils.fwsplit(line, ',')
                    elif linefmt == 'texttab':
                        lineinfo = miscutils.fwsplit(line, '\t')
                    elif linefmt == 'textsp':
                        lineinfo = miscutils.fwsplit(line, ' ')
                    else:
                        miscutils.fwdie('Error:  unknown linefmt (%s)' % linefmt, 1)

                    ldict = dict(zip(columns, lineinfo))
                    mylist.append(ldict)
            lines = queryutils.convert_single_files_to_lines(mylist)            
            mywcl = wcl.WCL(lines)

        return mywcl
        


def main():
    """ entry point """

    parser = argparse.ArgumentParser(description='Generic wrapper for LSST')
    parser.add_argument('inputwcl', nargs=1, action='store')
    args = parser.parse_args(sys.argv[1:])

    bwrap = GenWrapLSST(args.inputwcl[0])
    bwrap.run_wrapper()
    bwrap.write_outputwcl()
    sys.exit(bwrap.get_status())

if __name__ == "__main__":
    main()
