#!/usr/bin/env python

"""
Generic filetype management class used to do filetype specific tasks
     such as metadata and content ingestion
"""


from datetime import datetime
from collections import OrderedDict
import pyfits
import os
import re

import despydmdb.dmdb_defs as dmdbdefs
from filemgmt.ftmgmt_genfits import FtMgmtGenFits
from despymisc import miscutils
from despyfitsutils import fitsutils
import despyfitsutils.fits_special_metadata as spmeta


class FtMgmtHSCRaw(FtMgmtGenFits):
    """  Class for managing an HSC raw filetype (get metadata, update metadata, etc) """


#HSC CREATE TABLE raw (id integer primary key autoincrement, taiObs text,expId text,pointing int,dataType text,visit int,dateObs text,frameId text,filter text,field text,pa double,expTime double,ccdTemp double,ccd int,proposal text,config text,autoguider int, unique(visit,ccd));
#DES CREATE TABLE raw (id integer primary key autoincrement, hdu int,instcal text,wtmap text,visit int,taiObs text,filter text,ccdnum int,dqmask text,date text,ccd int,expTime double, unique(visit,ccdnum));

#HSC CREATE TABLE raw_visit (visit int,field text,filter text,dateObs text,taiObs text, unique(visit));
#DES CREATE TABLE raw_visit (visit int,date text,filter text, unique(visit));

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

        dbq = "select r.filename from image r, %s g where r.filename=g.filename" % \
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
        #hdulist = pyfits.open(fullname, 'update')
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
                    myvals = self._override_vals(fullname, hdulist, hdname)
                    for funckey in list(hddict[status_sect]['c'].keys()):
                        if funckey in myvals:
                            metadata[funckey] = myvals[funckey]
                        else:
                            try:
                                specmf = getattr(spmeta, 'func_%s' % funckey.lower())
                            except AttributeError:
                                miscutils.fwdebug_print(
                                    "WARN: Couldn't find func_%s in despyfits.fits_special_metadata" % (funckey))

                            try:
                                val = specmf(fullname, hdulist, hdname)
                                metadata[funckey] = val
                            except KeyError:
                                if miscutils.fwdebug_check(1, 'FTMGMT_DEBUG'):
                                    miscutils.fwdebug_print(
                                        "INFO: couldn't create value for key %s in %s header of file %s" % (funckey, hdname, fullname))

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

        metadata = OrderedDict()
        datadef = OrderedDict()
        for key in metakeys:
            if miscutils.fwdebug_check(6, 'FTMGMT_DEBUG'):
                miscutils.fwdebug_print("INFO: key=%s" % (key))
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
    def _override_vals(cls, fullname, hdulist, hdname):

        myvals = {}

        object = fitsutils.get_hdr_value(hdulist, 'OBJECT', hdname)
        myvals['field'] = cls.translate_field(object)

        dateobs = fitsutils.get_hdr_value(hdulist, 'DATE-OBS', hdname)
        myvals['taiobs'] = dateobs

        detId = fitsutils.get_hdr_value(hdulist, 'DET-ID', hdname)
        myvals['ccd'] = int(detId)

        expId = fitsutils.get_hdr_value(hdulist, 'EXP-ID', hdname)
        frameId = fitsutils.get_hdr_value(hdulist, 'FRAMEID', hdname)
        myvals['visit'] = cls.translate_visit(expId, frameId)

        filter = fitsutils.get_hdr_value(hdulist, 'FILTER01', hdname)
        myvals['filter'] = cls.translate_filter(filter)
        myvals['band'] = myvals['filter'][-1]

        mjd = fitsutils.get_hdr_value(hdulist, 'MJD', hdname)
        myvals['pointing'] = cls.getTjd(mjd)

        return myvals

    ######################################################################
    ######################################################################
    ######################################################################
    # copied from python/lsst/obs/subaru/ingest.py and then modified to remove
    # dependence upon md object

    @classmethod
    def translate_field(self, field):
        if field == "#":
            field = "UNKNOWN"
        # replacing inappropriate characters for file path and upper()
        field = re.sub(r'\W', '_', field).upper()

        return field

    @classmethod
    def translate_visit(self, expId, frameId):
        m = re.search("^HSCE(\d{8})$", expId)  # 2016-06-14 and new scheme
        if m:
            return int(m.group(1))

        # Fallback to old scheme
        m = re.search("^HSC([A-Z])(\d{6})00$", expId)
        if not m:
            raise RuntimeError("Unable to interpret EXP-ID: %s" % expId)
        letter, visit = m.groups()
        visit = int(visit)
        if visit == 0:
            # Don't believe it
            m = re.search("^HSC([A-Z])(\d{6})\d{2}$", frameId)
            if not m:
                raise RuntimeError("Unable to interpret FRAMEID: %s" % frameId)
            letter, visit = m.groups()
            visit = int(visit)
            if visit % 2:  # Odd?
                visit -= 1
        return visit + 1000000*(ord(letter) - ord("A"))

    @classmethod
    def translate_filter(self, filter):
        """Want upper-case filter names"""
        # filter01
        try:
            return filter.strip().upper()
        except:
            return "Unrecognized"

    @classmethod
    def getTjd(self, mjd):
        """Return truncated (modified) Julian Date"""
        #return int(mjd) - self.DAY0

        DAY0 = 55927  # Zero point for  2012-01-01  51544 -> 2000-01-01
        return int(mjd) - DAY0

    #@classmethod
    #def translate_pointing(self, md):
    #    """This value was originally called 'pointing', and intended to be used
    #    to identify a logical group of exposures.  It has evolved to simply be
    #    a form of truncated Modified Julian Date, and is called 'visitID' in
    #    some versions of the code.  However, we retain the name 'pointing' for
    #    backward compatibility.
    #    """
    #    try:
    #        return self.getTjd(md)
    #    except:
    #        pass
    #
    #    try:
    #        dateobs = md.get("DATE-OBS")
    #        m = re.search(r'(\d{4})-(\d{2})-(\d{2})', dateobs)
    #        year, month, day = m.groups()
    #        obsday = datetime.datetime(int(year), int(month), int(day), 0, 0, 0)
    #        mjd = datetime2mjd(obsday)
    #        return int(mjd) - self.DAY0
    #    except:
    #        pass
    #
    #    self.log.warn("Unable to determine suitable 'pointing' value; using 0")
    #    return 0
