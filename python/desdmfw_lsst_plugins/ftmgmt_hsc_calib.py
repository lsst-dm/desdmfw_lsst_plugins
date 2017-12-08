#!/usr/bin/env python

"""
"""


from datetime import datetime, timedelta
from collections import OrderedDict
import pyfits
import os
import re

import despydmdb.dmdb_defs as dmdbdefs
from filemgmt.ftmgmt_genfits import FtMgmtGenFits
from despymisc import miscutils
from despyfitsutils import fitsutils
import despyfitsutils.fits_special_metadata as spmeta


class FtMgmtHSCCalib(FtMgmtGenFits):
    """  Class for managing an HSC calib filetype (get metadata, update metadata, etc) """

    ######################################################################
    def __init__(self, filetype, dbh, config, filepat=None):
        """ Initialize object """
        # config must have filetype_metadata, file_header_info, keywords_file (OPT)
        FtMgmtGenFits.__init__(self, filetype, dbh, config, filepat)

    ######################################################################
    def has_contents_ingested(self, listfullnames):
        """ Check if exposure has row in rasicam_decam table """

        assert isinstance(listfullnames, list)

        # assume uncompressed and compressed files have same metadata
        # choosing either doesn't matter
        byfilename = {}
        for fname in listfullnames:
            filename = miscutils.parse_fullname(fname, miscutils.CU_PARSE_FILENAME)
            byfilename[filename] = fname

        self.dbh.empty_gtt(dmdbdefs.DB_GTT_FILENAME)
        self.dbh.load_filename_gtt(list(byfilename.keys()))

        dbq = "select r.filename from calibration r, %s g where r.filename=g.filename" % \
            (dmdbdefs.DB_GTT_FILENAME)
        curs = self.dbh.cursor()
        curs.execute(dbq)

        results = {}
        for row in curs:
            results[byfilename[row[0]]] = True
        for fname in listfullnames:
            if fname not in results:
                results[fname] = False

        self.dbh.empty_gtt(dmdbdefs.DB_GTT_FILENAME)

        return results

    ######################################################################
    def perform_metadata_tasks(self, fullname, do_update, update_info):
        """ Read metadata from file, updating file values """

        if miscutils.fwdebug_check(3, 'FTMGMT_DEBUG'):
            miscutils.fwdebug_print("INFO: beg")

        # open file
        primary_hdr = pyfits.getheader(fullname, 0)
        prihdu = pyfits.PrimaryHDU(header=primary_hdr)
        hdulist = pyfits.HDUList([prihdu])
        #import lsst.afw.image as afwImage
        #md = afwImage.readMetadata(filename, extnum)

        # read metadata and call any special calc functions
        metadata, _ = self._gather_metadata_file(fullname, hdulist=hdulist)
        if miscutils.fwdebug_check(6, 'FTMGMT_DEBUG'):
            miscutils.fwdebug_print("INFO: file=%s" % (fullname))

        # call function to update headers
        if do_update:
            miscutils.fwdebug_print("WARN: cannot update a raw file's metadata")

        # close file
        hdulist.close()

        if miscutils.fwdebug_check(3, 'FTMGMT_DEBUG'):
            miscutils.fwdebug_print("INFO: end")
        return metadata

    ######################################################################
    def ingest_contents(self, listfullnames, **kwargs):
        """ Ingest data into non-metadata table - raw_visit """
        # CREATE TABLE raw_visit (visit int,field text,filter text,dateObs text,taiObs text, unique(visit));

#        assert isinstance(listfullnames, list)
#
#        dbtable = 'raw_visit'
#
#        for fullname in listfullnames:
#            if not os.path.isfile(fullname):
#                raise OSError("Exposure file not found: '%s'" % fullname)
#
#            filename = miscutils.parse_fullname(fullname, miscutils.CU_PARSE_FILENAME)
#
#            primary_hdr = None
#            if 'prihdr' in kwargs:
#                primary_hdr = kwargs['prihdr']
#            elif 'hdulist' in kwargs:
#                hdulist = kwargs['hdulist']
#                primary_hdr = hdulist[0].header
#            else:
#                primary_hdr = pyfits.getheader(fullname, 0)
#
#            row = get_vals_from_header(primary_hdr)
#            row['filename'] = filename
#            row['source'] = 'HEADER'
#            row['analyst'] = 'DTS.ingest'
#
#            if len(row) > 0:
#                self.dbh.basic_insert_row(dbtable, row)
#            else:
#                raise Exception("No RASICAM header keywords identified for %s" % filename)
#
#
    ######################################################################
    def _gather_metadata_file(self, fullname, **kwargs):
        """ Gather metadata for a single file """

        if miscutils.fwdebug_check(3, 'FTMGMT_DEBUG'):
            miscutils.fwdebug_print("INFO: file=%s" % (fullname))

        hdulist = kwargs['hdulist']

        metadata = OrderedDict()
        datadef = OrderedDict()

        metadefs = self.config['filetype_metadata'][self.filetype]
        for hdname, hddict in list(metadefs['hdus'].items()):
            for status_sect in hddict:  # don't worry about missing here, ingest catches
                # get value from filename
                if 'f' in hddict[status_sect]:
                    metakeys = list(hddict[status_sect]['f'].keys())
                    mdata2 = self._gather_metadata_from_filename(fullname, metakeys)
                    metadata.update(mdata2)

                # get value from wcl/config
                if 'w' in hddict[status_sect]:
                    metakeys = list(hddict[status_sect]['w'].keys())
                    mdata2 = self._gather_metadata_from_config(fullname, metakeys)
                    metadata.update(mdata2)

                # get value directly from header
                if 'h' in hddict[status_sect]:
                    if miscutils.fwdebug_check(3, 'FTMGMT_DEBUG'):
                        miscutils.fwdebug_print("INFO: headers=%s" %
                                                (list(hddict[status_sect]['h'].keys())))
                    metakeys = list(hddict[status_sect]['h'].keys())
                    mdata2, ddef2 = self._gather_metadata_from_header(fullname, hdulist,
                                                                      hdname, metakeys)
                    metadata.update(mdata2)
                    datadef.update(ddef2)

                # calculate value from different header values(s)
                if 'c' in hddict[status_sect]:
                    myvals = self._override_vals(hdulist, hdname, fullname)
                    for funckey in list(hddict[status_sect]['c'].keys()):
                        #print funckey
                        if funckey in myvals:
                            metadata[funckey] = myvals[funckey]
                        else:
                            #print funckey, "not in myvals", myvals.keys()
                            try:
                                specmf = getattr(spmeta, 'func_%s' % funckey.lower())
                                try:
                                    val = specmf(fullname, hdulist, hdname)
                                    metadata[funckey] = val
                                except KeyError:
                                    if miscutils.fwdebug_check(1, 'FTMGMT_DEBUG'):
                                        miscutils.fwdebug_print(
                                            "INFO: couldn't create value for key %s in %s header of file %s" % (funckey, hdname, fullname))
                            except AttributeError:
                                miscutils.fwdebug_print(
                                    "WARN: Couldn't find func_%s in despyfits.fits_special_metadata" % (funckey))

                # copy value from 1 hdu to primary
                if 'p' in hddict[status_sect]:
                    metakeys = list(hddict[status_sect]['p'].keys())
                    mdata2, ddef2 = self._gather_metadata_from_header(fullname, hdulist,
                                                                      hdname, metakeys)
                    #print 'ddef2 = ', ddef2
                    metadata.update(mdata2)
                    datadef.update(ddef2)

        if miscutils.fwdebug_check(3, 'FTMGMT_DEBUG'):
            miscutils.fwdebug_print("INFO: metadata = %s" % metadata)
            miscutils.fwdebug_print("INFO: datadef = %s" % datadef)
            miscutils.fwdebug_print("INFO: end")
        return metadata, datadef

    ######################################################################
    @classmethod
    def _gather_metadata_from_header(cls, fullname, hdulist, hdname, metakeys):
        """ Get values from config """

        myvals = cls._override_vals(hdulist, hdname, fullname)

        metadata = OrderedDict()
        datadef = OrderedDict()
        for key in metakeys:
            if miscutils.fwdebug_check(6, 'FTMGMT_DEBUG'):
                miscutils.fwdebug_print("INFO: key=%s" % (key))

            if key in myvals:
                metadata[key] = myvals[key]
            else:
                try:
                    metadata[key] = fitsutils.get_hdr_value(hdulist, key.upper(), hdname)
                    datadef[key] = fitsutils.get_hdr_extra(hdulist, key.upper(), hdname)
                except KeyError:
                    if miscutils.fwdebug_check(1, 'FTMGMT_DEBUG'):
                        miscutils.fwdebug_print("INFO: didn't find key %s in %s header of file %s" %
                                                (key, hdname, fullname))

        return metadata, datadef

    ######################################################################
    @classmethod
    def _override_vals(cls, hdulist, hdname, fullname):

        #filter=(\S+) calibDate=(\d\d\d\d-\d\d-\d\d) ccd=(\d+)   but order can change
        myvals = {'camsym': 'H'}

        try:
            calib_id = fitsutils.get_hdr_value(hdulist, 'CALIB_ID', hdname)

            for field in ('filter', 'calibDate', 'ccd'):
                match = re.search(".*%s=(\S+)" % field, calib_id)
                if match:
                    myvals[field] = match.groups()[0]
                else:
                    raise ValueError('Invalid CALIB_ID when looking for %s: %s (%s)' %
                                     (field, calib_id, fullname))

                #if m.group(1).upper() != 'NONE':
                #    myvals['band'] = m.group(1)[-1]
                #myvals['validstart'] = datetime.strptime(myvals['calibdate'], "%Y-%m-%d") - timedelta(6*30)
                #myvals['validend'] = datetime.strptime(myvals['calibdate'], "%Y-%m-%d") + timedelta(6*30)
                #print "MMG", myvals
        except:  # TODO: need to figure out exact error to pass
            raise

        return myvals
