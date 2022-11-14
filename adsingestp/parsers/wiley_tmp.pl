#!/proj/ads/soft/utils/bin/perl5.8.1
#
# $Id: wiley.pl,v 1.5 1994/11/22 22:50:20 stern Exp stern $
#
# This program is part of the NASA Astrophysics Data System
# abstract service loading/indexing procedure.
#
# Copyright (C): 1994, 1995 Smithsonian Astrophysical Observatory.
# You may do anything you like with this file except remove
# this copyright.  The Smithsonian Astrophysical Observatory
# makes no representations about the suitability of this
# software for any purpose.  It is provided "as is" without
# express or implied warranty.  It may not be incorporated into
# commercial products without permission of the Smithsonian
# Astrophysical Observatory.
#
#

# these first two statements are needed to make sure
# we are using a unicode-aware perl and that none of the
# modules used are pulled from the old library tree
require 5.8.1;
no lib $ENV{PERL5LIB};

use strict;
my $script = $0;

use Encode;
use Search::Dict;
use ADS::Abstracts::Load;
use ADS::Abstracts::Accnos;
use ADS::Abstracts::Entities;
use ADS::Abstracts::IO;
use ADS::Authors::Names;

my $entity_enc = ADS::Abstracts::Entities::Encoder->new() or
    die "$script: cannot create entity encoder!";

##############################################################################
# Name of Program: wiley.pl
#
# Author:          Carolyn Stern Grant, May, 2002
#
# Description:     Extracts abstracts from xml files provided by Blackwell
#                  and writes the info into the abstract database.
#
# Inputs:          Requires the following data:
#                  * wiley.txt file containing the Blackwell data.
#                  * "./accno.config" which specifies what are the starting
#                    numbers for new accnos being assigned.
#
# Output:          Outputs two sets of data:
#                    1) data to be stored in text files is written into files
#                       named by accession number.  Files are stored in sub-
#                       directories named by the accession number prefix (e.g.,
#                       J92).
#                    2) updated master.list and codes files
#
##############################################################################

##################
# Global variables
##################
my $REF_DIR = "/proj/ads/references/sources";
my $FULL_DIR = "/proj/ads/fulltext/sources";
my $DOI  = "/proj/ads/abstracts/config/links/DOI/Wiley.dat";
my $MASTER_FILE = "/proj/ads/abstracts/phy/update/master.list";       # default name for output master list
my $MASTER_TMP = "./master.tmp";       # default name for output master list
my $FULLFILE = "/proj/ads/abstracts/config/links/fulltext/Wiley.tab";
my $SORT_DIR = "/proj/ads/adstmp";      # tmp dir for sorting
my $ENTITIES = "/proj/ads/abstracts/data/AJ/entities.tbl"; # sgml translations
my $abssuffix = ".abs";

my @AFFIL = ("AA","AB","AC","AD","AE","AF","AG","AH","AI","AJ","AK","AL","AM","AN","AO","AP","AQ","AR","AS","AT","AU","AV","AW","AX","AY","AZ","BA","BB","BC","BD","BE","BF","BG","BH","BI","BJ","BK","BL","BM","BN","BO","BP","BQ","BR","BS","BT","BU","BV","BW","BX","BY","BZ","CA","CB","CC","CD","CE","CF","CG","CH","CI","CJ","CK","CL","CM","CN","CO","CP","CQ","CR","CS","CT","CU","CV","CW","CX","CY","CZ","DA","DB","DC","DD","DE","DF","DG","DH","DI","DJ","DK","DL","DM","DN","DO","DP","DQ","DR","DS","DT","DU","DV","DW","DX","DY","DZ","EA","EB","EC","EE","EE","EF","EG","EH","EI","EJ","EK","EL","EM","EN","EO","EP","EQ","ER","ES","ET","EU","EV","EW","EX","EY","EZ","FA","FB","FC","FD","FE","FF","FG","FH","FI","FJ","FK","FL","FM","FN","FO","FP","FQ","FR","FS","FT","FU","FV","FW","FX","FY","FZ","GA","GB","GC","GD","GE","GF","GG","GH","GI","GJ","GK","GL","GM","GN","GO","GP","GQ","GR","GS","GT","GU","GV","GW","GX","GY","GZ","HA","HB","HC","HD","HE","HF","HG","HH","HI","HJ","HK","HL","HM","HN","HO","HP","HQ","HR","HS","HT","HU","HV","HW","HX","HY","HZ","IA","IB","IC","ID","IE","IF","IG","IH","II","IJ","IK","IL","IM","IN","IO","IP","IQ","IR","IS","IT","IU","IV","IW","IX","IY","IZ","JA","JB","JC","JD","JE","JF","JG","JH","JI","JJ","JK","JL","JM","JN","JO","JP","JQ","JR","JS","JT","JU","JV","JW","JX","JY","JZ","KA","KB","KC","KD","KE","KF","KG","KH","KI","KJ","KK","KL","KM","KN","KO","KP","KQ","KR","KS","KT","KU","KV","KW","KX","KY","KZ","LA","LB","LC","LD","LE","LF","LG","LH","LI","LJ","LK","LL","LM","LN","LO","LP","LQ","LR","LS","LT","LU","LV","LW","LX","LY","LZ","MA","MB","MC","MD","ME","M
F","MG","MH","MI","MJ","MK","ML","MM","MN","MO","MP","MQ","MR","MS","MT","MU","M
V","MW","MX","MY","MZ","NA","NB","NC","ND","NE","NF","NG","NH","NI","NJ","NK","N
L","NM","NN","NO","NP","NQ","NR","NS","NT","NU","NV","NW","NX","NY","NZ","OA","O
B","OC","OD","OE","OF","OG","OH","OI","OJ","OK","OL","OM","ON","OO","OP","OQ","OR","OS","OT","OU","OV","OW","OX","OY","OZ","PA","PB","PC","PD","PE","PF","PG","PH","PI","PJ","PK","PL","PM","PN","PO","PP","PQ","PR","PS","PT","PU","PV","PW","PX","PY","PZ","QA","QB","QC","QD","QE","QF","QG","QH","QI","QJ","QK","QL","QM","QN","QO","QP","QQ","QR","QS","QT","QU","QV","QW","QX","QY","QZ","RA","RB","RC","RD","RE","RF","RG","RH","RI","RJ","RK","RL","RM","RN","RO","RP","RQ","RR","RS","RT","RU","RV","RW","RX","RY","RZ","SA","SB","SC","SD","SE","SF","SG","SH","SI","SJ","SK","SL","SM","SN","SO","SP","SQ","SR","SS","ST","SU","SV","SW","SX","SY","SZ","TA","TB","TC","TD","TE","TF","TG","TH","TI","TJ","TK","TL","TM","TN","TO","TP","TQ","TR","TS","TT","TU","TV","TW","TX","TY","TZ","UA","UB","UC","UD","UE","UF","UG","UH","UI","UJ","UK","UL","UM","UN","UO","UP","UQ","UR","US","UT","UU","UV","UW","UX","UY","UZ","VA","VB","VC","VD","VE","VF","VG","VH","VI","VJ","VK","VL","VM","VN","VO","VP","VQ","VR","VS","VT","VU","VV","VW","VX","VY","VZ","WA","WB","WC","WD","WE","WF","WG","WH","WI","WJ","WK","WL","WM","WN","WO","WP","WQ","WR","WS","WT","WU","WV","WW","WX","WY","WZ","XA","XB","XC","XD","XE","XF","XG","XH","XI","XJ","XK","XL","XM","XN","XO","XP","XQ","XR","XS","XT","XU","XV","XW","XX","XY","XZ","YA","YB","YC","YD","YE","YF","YG","YH","YI","YJ","YK","YL","YM","YN","YO","YP","YQ","YR","YS","YT","YU","YV","YW","YX","YY","YZ","ZA","ZB","ZC","ZD","ZE","ZF","ZG","ZH","ZI","ZJ","ZK","ZL","ZM","ZN","ZO","ZP","ZQ","ZR","ZS","ZT","ZU","ZV","ZW","ZX","ZY","ZZ");

my %BIBSTEMS = ('Global Biogeochemical Cycles','GBioC',
           'Astronomische Nachrichten','AN...',
           'AGU Advances','AGUA.',
           'Geochemistry, Geophysics, Geosystems','GGG..',
           'Geophysical Research Letters','GeoRL',
           'Earth\'s Future','EaFut',
           'Earth and Space Science','E&SS.',
           'Eos, Transactions American Geophysical Union','EOSTr',
           'Journal of Advances in Modeling Earth Systems','JAMES',
           'Journal of Geophysical Research','JGR..',
           'Journal of Geophysical Research: Space Physics','JGRA.',
           'Journal of Geophysical Research: Solid Earth','JGRB.',
           'Journal of Geophysical Research: Oceans','JGRC.',
           'Journal of Geophysical Research: Atmospheres','JGRD.',
           'Journal of Geophysical Research: Planets','JGRE.',
           'Journal of Geophysical Research: Earth Surface','JGRF.',
           'Journal of Geophysical Research: Biogeosciences','JGRG.',
           'Meteoritics &amp; Planetary Science','M&PS.',
           'Paleoceanography','PalOc',
           'Radio Science','RaSc.',
           'Reviews of Geophysics','RvGeo',
           'Space Weather','SpWea',
           'Tectonics','Tecto',
           'Water Resources Research','WRR..');

my %ISSUES = ('1','a',
           '2','b',
           '3','c',
           '4','d',
           '5','e',
           '6','f');

my($recs,$line,$oldtmp,$val,$b);
my($REF_FILE,$ELECTR,$JOURNAL,$FILE,$first,$noaff);
my(%info,%rec,%entities,$VOL,%doiarray,@bibarray);
my $ispage = 1;

#############################
#Parse command-line arguments
#############################
  &ParseArgs();

  $recs = 0;            # initialize record counter

my $date=`date +'%Y-%m-%d'`;
my $time=`date +'%H:%M:%S'`;
# printf("Beginning WILEY data extraction...%s\n",$time);

#################
# open master.list
#################
  open(my $master,"< $MASTER_FILE") || die "Cannot open file $MASTER_FILE for reading: $!\n";

#############################################
# open file of deletions and author abstracts
#############################################

  open(my $fullfile, ">> $FULLFILE") || die "Couldn't open $FULLFILE for appending: $!\n";
  open(my $entities, "< $ENTITIES") || die "Couldn't open $ENTITIES for reading: $!\n";
  open(my $doi, ">> $DOI") || die "Couldn't open $DOI for writing: $!\n";

###########################
# read in sgml translations
###########################
  while(<$entities>) {
      chop;
      my ($code,$ascii) = split(/\t/,$_);
      $entities{$code} = $ascii;
  }
  close($entities);

#######################################################################
# Open input file on the command line and read it into record of arrays
# where each record is begins with <HEAD> and ends with </HEAD>
#######################################################################
my($FULLDIR,$REFDIR);
while (defined(my $filename = shift(@ARGV))){ # while there are files left to read
  open($FILE,"< $filename")  || die "Couldn't open $filename file for reading:$!\n";
  undef($line);
  while(<$FILE>) {
      chop;
      $line = $line . $_;
  }
  close($FILE);

  # de-sgml, rewrite accents, and de-sgml again:
  $line = &DeSGML($line);
  $line =~ s/<\!-.*?>//g;     # remove comments
  $line =~ s/\r\;/\;/g;

  &ReadXMLRecord($line);      # read record data into global %recs array
  ++$recs;
  &ComputeDerivedFields(); # compute JNL etc.
  &AssignBibcode();           # assign bibcode
  &WriteOutput();           # Write out results to an output table and
                               # output files
  if(length($rec{'V'}) == 4 ) {
  } elsif(length($rec{'V'}) == 3 ) {
      $VOL = "0$rec{'V'}";
  } elsif(length($rec{'V'}) == 2 ) {
      $VOL = "00$rec{'V'}";
  } elsif(length($rec{'V'}) == 1 ) {
      $VOL = "000$rec{'V'}";
  }
  $rec{'BS'} =~ s/\&/+/g;
  $REFDIR = "$REF_DIR/$rec{'BS'}/$VOL";
  $REFDIR =~ s/\.//g;
  $FULLDIR = "$FULL_DIR/$rec{'BS'}/$VOL";
  $FULLDIR =~ s/\.//g;

  if (! -d $REFDIR) {
      mkdir($REFDIR,0755) || print STDERR "## Couldn't mkdir $REFDIR\n";
  }
  if (! -d $FULLDIR) {
      mkdir($FULLDIR,0755) || print STDERR "## Couldn't mkdir $FULLDIR\n";
  }
  &ReadCites($line);

    # full text handling
  system("cp -p $filename $FULLDIR") && die "Couldn't copy file $filename !\n";
  $filename =~ s/.*\/(.*)$/$1/;
  print $fullfile "$rec{'DOI'}\t$FULLDIR\/$filename\n";;
}

  close($master);
  close($fullfile);
  close(ELECTR);
  close($doi);

$time=`date +'%H:%M:%S'`;
# printf( "Done...processed $recs records...%s\n",$time);

exit(0);


##############################################################################
# Name of Module:  &ReadXMLRecord() - read in a record from the input line
#
# Description:     This reads in a record, extracts the data for all tags
#                  (mnemonics), and stores the data in a global associative
#                  array %rec that is indexed by the tag.
#
# Inputs:          * array with input records
#
# Output:          Loads the data into a %rec associative array.  The result
#                  is an array indexed by mnemonic tag containing the data
#                  values.
#
##############################################################################

sub ReadXMLRecord {

    my $line = $_[0];
    my ($au,$autstring,@lname,@finit,@auts,@a_ff,$affstring,@affils,@affarray);
    my ($surtitle,$head,$oneaff,$af,$af1,$af2,$af3,$af4,$newaut,$afforg);
    my ($i,$j,$k,%affarray,$id,$footnote,$footnotenum,$sectitle,$title,@aff);
    my (@orcid);

    undef(%rec);        # erase any previous contents of %rec

# journal
    if( $line =~ /<publicationMeta.*?<titleGroup>\s*<title.*?>(.+?)<\/title>/i ) {
        $rec{"J"} = $1;
        $rec{"BS"} = $BIBSTEMS{$rec{'J'}};
    } elsif( $line =~ /<publicationMeta.*?<titleGroup>\s*<title\stype=\"main\".*?\">(.+?)<\/title>/i ) {
        $rec{"J"} = $1;
        $rec{"BS"} = $BIBSTEMS{$rec{'J'}};
    }
# issue
    if( $line =~ /<numbering\stype=\"journalIssue\".*?>([A-Z]*\d+?)<\/numbering>/i ) {
        $rec{"I"} = $1;
    }
# volume
    if( $line =~ /<numbering\stype=\"journalVolume\".*?>(\w+?)<\/numbering>/i ) {
        $rec{"V"} = $1;
    } elsif( $line =~ /<numbering.*?type=\"journalVolume\".*?>(\w+?)<\/numbering>/i ) {
        $rec{"V"} = $1;
    }
# DOI
    if( $line =~ /<publicationMeta\slevel=\"unit\"\sposition=\"\d+\"\sstatus=\"forIssue\".*?>\s*<doi.*?>(.*?)<\/doi>/i ) {
        $rec{"DOI"} = $1;
    } elsif( $line =~ /<publicationMeta\saccessType=\"open\"\slevel=\"unit\"\sposition=\"\d+\"\sstatus=\"forIssue\".*?>\s*<doi.*?>(.*?)<\/doi>/i ) {
        $rec{"DOI"} = $1;
        $rec{"OPEN"} = 1;
    } elsif( $line =~ /<publicationMeta\slevel=\"unit"\stype=\"\S+\"\sstatus=\"\S+\".*?><doi.*?>(.*?)<\/doi>/i ) {
        $rec{"DOI"} = $1;
    } elsif( $line =~ /<publicationMeta\slevel=\"unit"\stype=\"\S+\"\sstatus=\"\S+\".*?><doi.*?>(.*?)<\/doi>/i ) {
        $rec{"DOI"} = $1;
    } elsif( $line =~ /<publicationMeta\slevel=\"unit\"\sposition=\"\d+\"\stype=\"\S+\"\sstatus=\"forIssue\".*?>\s*<doi.*?>(.*?)<\/doi>/i ) {
        $rec{"DOI"} = $1;
    } elsif( $line =~ /<publicationMeta.*?type=\"\S+\".*?><doi.*?>(.*?)<\/doi>/i ) {
        $rec{"DOI"} = $1;
    } elsif( $line =~ /<publicationMeta\slevel=\"unit\"\stype=\"\S+\"\sposition=\"\d+\"\sstatus=\"forIssue\">\s*<doi.*?>(.*?)<\/doi>/i ) {
        $rec{"DOI"} = $1;
    } elsif( $line =~ /<publicationMeta.*?status=\"forIssue\"><doi.*?>(.*?)<\/doi>/i ) {
        $rec{"DOI"} = $1;
    } elsif( $line =~ /<accessType=\"open\"\sstatus=\"forIssue\"><doi.*?>(.*?)<\/doi>/i ) {
        $rec{"DOI"} = $1;
        $rec{"OPEN"} = 1;
    } elsif( $line =~ /<doi\.*>(.*?)<\/doi>/i ) {
        $rec{"DOI"} = $1;
    }
    if( $line =~ /accessType=\"open\"/i ) {
        $rec{"OPEN"} = 1;
    }
# pubdate
    if( $line =~ /<coverDate\sstartDate=\"(\d\d\d\d)-(\d\d)-\d\d\">/ ) {
        $rec{"Y"} = $1;
        $rec{"PMO"} = $2;
    } elsif( $line =~ /<coverDate\sstartDate=\"(\d\d\d\d)-(\d\d)\">/ ) {
        $rec{"Y"} = $1;
        $rec{"PMO"} = $2;
    } elsif( $line =~ /<coverDate\sstartDate=\"(\d\d\d\d)\">/ ) {
        $rec{"Y"} = $1;
        $rec{"PMO"} = "00";
    } elsif( $line =~ /<date\stype=\"online-early\"\sdate=\"(\d+?)-(\d+)\">/ ) {
        $rec{"Y"} = $1;
        $rec{"PMO"} = $2;
    }
    if( $rec{"Y"} < 2000 ) {
        $rec{"PYR"} = $rec{"Y"} - 1900;
    } else {
        $rec{"PYR"} = $rec{"Y"} - 2000;
    }
    if( length($rec{"PYR"}) == 1 ) {
        $rec{"PYR"} = "0" . $rec{"PYR"};
    }
    if( length($rec{"PYR"}) == 0 ) {
        $rec{"PYR"} = "00";
    }
    if( length($rec{"PMO"}) == 1 ) {
        $rec{"PMO"} = "0" . $rec{"PMO"};
        }
    if( $rec{"BS"} =~ /JGR/ ) {                     # JGR back issues
        if( $rec{"Y"} < 2002 ) {
            $rec{"BS"} = "JGR..";
        }
    }
# page
    if( $line =~ /<numbering\stype=\"pageFirst\".*?>(.+?)<\/numbering>/i ) {
        $rec{"P"} = $1;
        $line =~ /<numbering\stype=\"pageLast\".*?>(.+?)<\/numbering>/i;
        $rec{"L"} = $1;
        $ispage = 1;
    } elsif( $line =~ /<id\stype=\"society\"\svalue=\"(.+?)\"\/>/i ) {
       $rec{"P"} = $1;
    } else {
#      print "could not parse page for $rec{'DOI'}\n";
    }
    if( $line =~ /<id\stype=\"eLocator\"\svalue="(.+?)\"\/>/i ) {
        $rec{'P'} = $1;
        $rec{'P'} = substr($rec{'P'},length($rec{'P'})-5,5);
        $ispage = 0;
    }
    if($rec{'P'} =~ /n\/a/) {
        if( $line =~ /<id\stype=\"eLocator\"\svalue="(.+?)\"\/>/i ) {
            $rec{'P'} = $1;
            $rec{'P'} = substr($rec{'P'},8,5);
            $ispage = 0;
        }
    } elsif($rec{'P'} =~ /^no$/) {
        if( $line =~ /<id\stype=\"eLocator\"\svalue="(.+?)\"\/>/i ) {
            $rec{'P'} = $1;
            $rec{'P'} = substr($rec{'P'},8,5);
            $ispage = 0;
        }
    } else {
#       print "could not parse page for $rec{'DOI'}\n";
    }
# title
    if( $line =~ /<contentMeta>.*?<titleGroup>\s*<title\stype=\"main\".*?>(.+?)<\/title>/i ) {
        $rec{"XTL"} = $1;
    } elsif( $line =~ /<titleGroup>\s*<title\stype=\"main\".*?>(.+?)<\/title>/i ) {
        $rec{"XTL"} = $1;
#   } elsif( $line =~ /<title\stype=\"main\".*?>(.+?)<\/title>/i ) {
#       $rec{"XTL"} = $1;
    }
    next if($rec{"XTL"} =~ /^Issue\sInformation$/g);
# section titles
    if( $line =~ /<title\stype=\"tocHeading1\">(.+?)<\/title>/i ) {
        $sectitle = $1;
    }
    if($sectitle =~ /News\sand\sViews/ ) {
            $title = "$sectitle: $rec{'XTL'}";
        $rec{'XTL'} = $title;
        } elsif($sectitle =~ /Obituaries/ ) {
            $title = "$sectitle: $rec{'XTL'}";
        $rec{'XTL'} = $title;
        } elsif($sectitle =~ /Society\sNews/ ) {
            $title = "$sectitle: $rec{'XTL'}";
        $rec{'XTL'} = $title;
        } elsif($sectitle =~ /Book\sReview/ ) {
            $title = "$sectitle: $rec{'XTL'}";
        $rec{'XTL'} = $title;
        }
    $rec{'XTL'} =~ s/<citation.*?bookTitle>(.*?)\,<\/bookTitle.*/$1/;

# subtitles??
    if( $rec{"XTL"} =~ s/<link\shref=\"\#(.*?)\"\/>// ) {
        $footnotenum = $1;
     }
     if( $footnotenum ) {
             if($line =~ /<noteGroup>(.*?)<\/noteGroup>/i ) {
                    $footnote = $1;
        }
        $footnote =~ s/<note\sxml\:id=\".*?\">//g;
        $footnote =~ s/<\/note>//g;
        $footnote =~ s/<p>//g;
        $footnote =~ s/<\/p>//g;
    }
    $rec{"XTL"} =~ s/^\s*//g;
    $rec{"XTL"} =~ s/<b>//ig;
    $rec{"XTL"} =~ s/<\/b>//ig;
    $rec{"XTL"} =~ s/<i>//ig;
    $rec{"XTL"} =~ s/<\/i>//ig;
        $rec{"XTL"} =~ s/<fi>(.*?)<\/fi>/$1/g;


# copyright
    $line =~ /<copyright(.+?)<\/copyright>/i;
    $rec{'COP'} = $1;
    $rec{'COP'} =~ s/^\s.*?>//g;
    $rec{'COP'} =~ s/^>//g;
# authors
    my $prefix;
    if( $line =~ /<creators>(.+?)<\/creators>/i ) {
        $autstring = $1;
        @auts = split(/<\/creator/,$autstring);
        if( $#auts == 0 ) {
            $auts[0] =~ s/<\/creator>//ig;
            if( $auts[0] =~ /<id\stype=\"orcid\"\svalue=\"https\:\/\/orcid\.org\/(.*?)\"/i ) {
                $orcid[0] = $1;
            }

            if( $auts[0] =~ /<givenNames>(.+)<\/givenNames>/i ) {
                $finit[0] = $1;
            }
            $auts[0] =~ /<familyName>(.+)<\/familyName>/i;
            $lname[0] = $1;
            if( $auts[0] =~ /<familyNamePrefix>(.+)<\/familyNamePrefix>/i ) {
                $prefix = $1;
                $lname[0] = $prefix . " " . $lname[0];
                undef $prefix ;
            }
            if( $lname[0] =~ /\S+\sJr/i ) {
                $lname[0] =~ s/,\sJr\.*//g;
                $lname[0] =~ s/\sJr\.*//g;
                $finit[0] = $finit[0] . ", Jr.";
            }
# print "$auts[0]\n";
            if( $auts[0] =~ s/affiliationRef=\"(.*?)\"//g ) {
                $aff[0] = $1;
                $aff[0] =~ s/[a-z]//g;
                $aff[0] =~ s/\#//g;
                $aff[0] =~ s/\s/\,/g;
            } elsif( $auts[0] =~ s/correspondenceRef=\"(.*?)\"//g ) {
                $aff[0] = $1;
                $aff[0] =~ s/[a-z]//g;
                $aff[0] =~ s/\#//g;
                $aff[0] =~ s/\s/\,/g;
            }
            if(! $lname[0] ) {
                if( $auts[0] =~ /<familyName>(.+)<\/familyName>/i ) {
                                $lname[0] = $1;
                }
            }
# print "auts0: $i $aff[0]:  $finit[0] $lname[0]\n";
        } else {
            for $i(0..$#auts) {
                $auts[$i] =~ s/<\/creator>//ig;
                if( $auts[$i] =~ /<id\stype=\"orcid\"\svalue=\"https\:\/\/orcid\.org\/(.*?)\"/i ) {
                    $orcid[$i] = $1;
                }

                if( $auts[$i] =~ /<givenNames>(.+)<\/givenNames>/i ) {
                    $finit[$i] = $1;
                }
                $auts[$i] =~ /<familyName>(.+)<\/familyName>/i;
                $lname[$i] = $1;
                if( $auts[$i] =~ /<familyNamePrefix>(.+)<\/familyNamePrefix>/i ) {
                    $prefix = $1;
                    $lname[$i] = $prefix . " " . $lname[$i];
                    undef $prefix ;
                }
                if( $lname[$i] =~ /\S+\sJr/i ) {
                    $lname[$i] =~ s/,\sJr\.*//g;
                    $lname[$i] =~ s/\sJr\.*//g;
                    $finit[$i] = $finit[$i] . ", Jr.";
                }
                if( $auts[$i] =~ s/affiliationRef=\"(.*?)\"//g ) {
                    $aff[$i] = $1;
#                   $aff[$i] =~ s/[a-z]//g;
                    $aff[$i] =~ s/\s/\,/g;
                    $aff[$i] =~ s/\#//g;
                } elsif( $auts[$i] =~ s/correspondenceRef=\"(.*?)\"//g ) {
                    $aff[$i] = $1;
#                   $aff[$i] =~ s/[a-z]//g;
                    $aff[$i] =~ s/\s/\,/g;
                    $aff[$i] =~ s/\#//g;
                }
                if(! $lname[$i] ) {
                    if( $auts[$i] =~ /<familyName>(.+)<\/familyName>/i ) {
                                    $lname[$i] = $1;
                    }
                }
# print "auts: $i $aff[$i]:  $finit[$i] $lname[$i]\n";
            }
        }
    } elsif( $line =~ /<creator\sxml:id=\"\S*\"><groupName>AGU<\/groupName><\/creator>/i ) {
        $rec{'AUT'} = "AGU";
    } else {
        $rec{'AUT'} = "";
    }
# print "auts: $aff[0]:  $finit[0] $lname[0]\n";
# affiliations
    if( $line =~ /<affiliationGroup.*?>(.*)<\/affiliationGroup>/i ) {
        $affstring = $1;
    } else {
        $noaff = 1;
    }
# print "$affstring\n";
    $affstring =~ s/E\-mail\://g;
    $affstring =~ s/email>/EMAIL>/g;
# now fielded
#   $affstring =~ s/<affiliation\stype=\"organization\"\s.*?>//g;
    $affstring =~ s/\scountryCode=\"\S+\"\stype=\"organization\"//g;
    $affstring =~ s/<orgDiv>(.*?)<\/orgDiv>/$1, /g;
    $affstring =~ s/<orgName>(.*?)<\/orgName>/$1, /g;
    $affstring =~ s/<city>(.*?)<\/city>/$1, /g;
    $affstring =~ s/<countryPart>(.*?)<\/countryPart>/$1 /g;
    $affstring =~ s/<postCode>(.*?)<\/postCode>/$1 /g;
    $affstring =~ s/<address>(.*?)<\/address>/$1/g;
    $affstring =~ s/<street>(.*?)<\/street>/$1 /g;
    $affstring =~ s/<country>(.*?)<\/country>/$1/g;
# print "$affstring\n";
      if( $affstring =~ /<\/affiliation/  ) {
          @affils = split(/<\/affiliation>/,$affstring);
    } else {
        $oneaff = 1;
    }
    if($#affils == 0 ) {
        $affstring =~ s/<affiliation\stype=\"organization\"\s.*?>//g;
        $affstring =~ s/<affiliation\sxml:id=\"\S+\">//g;
        $affstring =~ s/<affiliation\sxml:id=\"\S+\"\stype=\"organization\">//g;
        $affstring =~ s/<unparsedAffiliation>//ig;
        $affstring =~ s/<\/unparsedAffiliation>//ig;
        if( $affstring =~ /<affiliation.*?xml:id=\"(\S+)\"\scountryCode=\"\S+\">(.*?)$/ig ) {
            $id = $1;
            $afforg = $2;
            $id =~ s/a//g;
            $affarray{$id} = $afforg;
            $affarray{$id} =~ s/<\/affiliation>//g;
                        $affstring = $affarray{$id};
        } else {
            $oneaff = 1;
        }
    } else {
        for $j(0..$#affils) {
            $affils[$j] =~ s/<unparsedAffiliation>//ig;
            $affils[$j] =~ s/<\/unparsedAffiliation>//ig;
            $affils[$j] =~ s/<fr>//g;
            $affils[$j] =~ s/<\/fr>//g;
# print "$j: $affils[$j]\n";
            if( $affils[$j] =~ /<affiliation.*?xml:id=\"(\S+)\"\scountryCode=.*?>(.*?)$/ig ) {
                $id = $1;
                $afforg = $2;
#               $id =~ s/[a-z]//g;
                $id =~ s/\#//g;
                $affarray{$id} = $afforg;
                $affarray{$id} =~ s/^\s*|\s*$//g;
                          $affstring = $affarray{$id};
# print "$j: $id: $affarray{$id}\n";
            } elsif( $affils[$j] =~ /<affiliation.*?xml:id=\"(\S+)\">(.*?)$/ig ) {
                $id = $1;
                $afforg = $2;
#               $id =~ s/[a-z]//g;
                $id =~ s/\#//g;
                $affarray{$id} = $afforg;
                $affarray{$id} =~ s/^\s*|\s*$//g;
                $affarray{$id} =~ s/<\/affiliation>//g;
                          $affstring = $affarray{$id};
# print "$j: $id: $affarray{$id}\n";
            } elsif( $affils[$j] =~ /<affiliation.*?xml:id=\"(\S+)\"\stype=\"organization\">(.*?)$/ig ) {
                $id = $1;
                $afforg = $2;
#               $id =~ s/[a-z]//g;
                $id =~ s/\#//g;
                $affarray{$id} = $afforg;
                $affarray{$id} =~ s/^\s*|\s*$//g;
                $affarray{$id} =~ s/<\/affiliation>//g;
                          $affstring = $affarray{$id};
# print "$j: $id: $affarray{$id}\n";
            }
        }

    }
    if($#auts == 0) {
        $rec{"AUT"} = $lname[0] . ", " . $finit[0];
        if(! $oneaff ) {
            if( $aff[0] =~ /\,/ ) {
                ($af1,$af2) = split(/\,/,$aff[0]);
                if($orcid[0]) {
                    $rec{"AFF"} = "AA(" . $affarray{$af1} . "\; " . $affarray{$af2} . " <ID system=\"ORCID\">$orcid[0]<\/ID>)";
                } else {
                    $rec{"AFF"} = "AA(" . $affarray{$af1} . "\; " . $affarray{$af2}. ")";
                }
            } else {
                $rec{"AFF"} = "AA(" . $affarray{$aff[0]} . ")";
            }
        } else {
            $affstring =~ s/<affiliation.*?xml:id=\".*?\"\scountryCode=\"\S+\">\s*//g;
            $affstring =~ s/<affiliation.*?xml:id=\".*?\">\s*//g;
            $affstring =~ s/<\/affiliation>//g;
            $rec{"AFF"} = "AA($affstring)";
        }
    } else {
        for $k( 0..$#auts) {
            $au = $lname[$k] . ", " . $finit[$k];
            next if($au =~ /^\#/ );
            if(! $oneaff ) {
                if( $aff[$k] =~ /\,/ ) {
#  print "in here, $aff[$k]\n";
                    ($af1,$af2,$af3,$af4) = split(/\,/,$aff[$k]);
                    if($af4) {
                        $af = $AFFIL[$k] . "(" . $affarray{$af1} . "\; " . $affarray{$af2} . "\; " . $affarray{$af3} . "\; " . $affarray{$af4} . ")";
                    } elsif($af3 ) {
                        $af = $AFFIL[$k] . "(" . $affarray{$af1} . "\; " . $affarray{$af2} . "\; " . $affarray{$af3} . ")";
                    } else {
                        $af = $AFFIL[$k] . "(" . $affarray{$af1} . "\; " . $affarray{$af2} . ")";
                    }
                } else {
# print "in here, $aff[$k]: $affarray{$aff[$k]}\n";
                    $af = $AFFIL[$k] . "(" . $affarray{$aff[$k]} . ")";
                }
                if($orcid[$k]) {
                    $af =~ s/\)$/ <ID system=\"ORCID\">$orcid[$k]<\/ID>\)/;
                }
            } else {
                $affstring =~ s/<affiliation.*?xml:id=\".*?\"\scountryCode=\"\S+\">\s*//g;
                $affstring =~ s/<\/affiliation>//g;
                $af = $AFFIL[$k] . "(" . $affstring . ")";
                if($orcid[$k]) {
                    $af =~ s/\)$/ <ID system=\"ORCID\">$orcid[$k]<\/ID>\)/;
                }
            }
# print "$au\n";
            if( $rec{"AUT"} ) {
                $rec{"AUT"} .= "; $au";
            } else {
                $rec{"AUT"} = $au;
            }
            if( $rec{"AFF"} ) {
                $rec{"AFF"} .= ", $af";
            } else {
                $rec{"AFF"} = $af;
            }
        }
    }
    $oneaff = 0;
    $rec{"AUT"}=~ s/\&dOT\;//g;
    $newaut = &ADS::Authors::Names::Parse($rec{"AUT"},order => "lastfirst", sep => "; ", flsep => ",");
    $rec{"AUT"} = $newaut;
    $rec{'AUT'} =~ s/\&lt\;sup\&gt\;c\&lt\;\/sup\&gt\;/c/ig;
    $rec{'AUT'} =~ s/\&acirc\;\&\#128\;\&nacute\;/-/ig;
    $rec{'AUT'} =~ s/\&acirc\;\s/ /ig;

    $rec{"AFF"} =~ s/\s\&\s/ \&amp\; /g;
    $rec{"AFF"} =~ s/<span\sid=\"\S+\">//g;
    $rec{"AFF"} =~ s/\(\s/\(/g;
    $rec{'AFF'} =~ s/<affiliation.*?xml:id=\".*?\">\s*//g;
    if( $noaff ) {
        undef($rec{'AFF'});
        $noaff = 0;
    }
# print "$rec{'AFF'}\n";

# abstract
    if( $line =~ /<abstract\stype=\"main\"\sxml:.*?\">\s*(.*?)<\/abstract>/i ) {
        $rec{"ABS"} = $1;
    } elsif( $line =~ /<abstract\stype=\"main\"\sxml:lang=\"\S+\">\s*<title\stype=\"main\">ABSTRACT<\/title>(.*?)<\/abstract>/i ) {
        $rec{"ABS"} = $1;
    } elsif( $line =~ /<abstract\stype=\"main\"\sxml:lang=\"\S+\">(.*?)<\/abstract>/i ) {
        $rec{"ABS"} = $1;
    } elsif( $line =~ /<abstract\stype=\"main\">\s*<title\stype=\"main\">ABSTRACT<\/title>\s*<p>(.*)<\/p><\/abstract>/i ) {
        $rec{"ABS"} = $1;
    } elsif( $line =~ /<abstract\stype=\"main\"><.*?>(.*)<\/abstract>/i ) {
        $rec{"ABS"} = $1;
    } elsif( $line =~ /<abstractGroup>(.*)<\/abstractGroup>/i ) {
        $rec{"ABS"} = $1;
    } else {
        $rec{"ABS"} = "Not Available";
    }
    $rec{"ABS"} =~ s/<abstract\stype=\"short\".*$//g;
    $rec{"ABS"} =~ s/<\/p>\s*<p>/\n\n/ig;
    $rec{"ABS"} =~ s/<p>//ig;
    $rec{"ABS"} =~ s/<\/p>//ig;
    $rec{"ABS"} =~ s/<span\stype=\"\S+\">//g;
    $rec{"ABS"} =~ s/<\/span>//ig;
    $rec{"ABS"} =~ s/<link\shref=\"\S+\">//g;
    $rec{"ABS"} =~ s/<\/link>//ig;
        $rec{"ABS"} =~ s/<abstract\stype=\"main\"><p\sxml:id.*?>//g;
        $rec{"ABS"} =~ s/<abstract\stype=\"main\"\s\sxml:id.*?>//g;
        $rec{"ABS"} =~ s/<abstract\stype=\"main\">//g;
        $rec{"ABS"} =~ s/<abstract\stype=\"main\">//g;
        $rec{"ABS"} =~ s/<abstract\stype=\"main\"><title\stype=\"main\">ABSTRACT<\/title>//g;
        $rec{"ABS"} =~ s/<abstract\stype=\"short\">.*//g;
        $rec{"ABS"} =~ s/<abstract\stype=\"main\"><title\stype=\"main\">Abstract<p
\slabel=\"1\"//g;
        $rec{"ABS"} =~ s/<\/abstract>//g;
    $rec{"ABS"} =~ s/<p\sxml:id=\"\S+\".*?>/ /g;
        $rec{"ABS"} =~ s/<inlineGraphic.*?\"\/>/?/g;
        $rec{"ABS"} =~ s/<p\slabel=\"\S+\">//g;
        $rec{"ABS"} =~ s/<title\stype=\"main\">Abstract<p]slabel=\"\S+\">//g;
    if($footnote) {
        $rec{"ABS"} = $rec{"ABS"} . " " . $footnote;
    }
    $rec{"ABS"} = DeSGML($rec{'ABS'});
        $rec{"ABS"} =~ s/^Abstract.*?\s//g;
        $rec{"ABS"} =~ s/<abstract\stype=\"main\"\sxml:id=\S+><title type=\"main\">Abstract<\/title>//g;
        $rec{"ABS"} =~ s/<section\sxml:id=\".*?\">\s*/ /g;
        $rec{"ABS"} =~ s/<title\stype=\"main\">.*?<\/title>\s*//g;
        $rec{"ABS"} =~ s/<\/section><section.*?>/\n\n/g;
        $rec{"ABS"} =~ s/<annotation\sencoding=\"application\/x-tex\">//g;
        $rec{"ABS"} =~ s/<semantics>//g;
        $rec{"ABS"} =~ s/<\/semantics>//g;
        $rec{"ABS"} =~ s/<\/annotation>//g;
        $rec{"ABS"} =~ s/<url\shref/<A href/g;
        $rec{"ABS"} =~ s/<fi>(.*?)<\/fi>/$1/g;
        $rec{"ABS"} =~ s/<\/url>/<\/A>/g;
# keywords
    if( $line =~ /<keywordGroup\s.*?>(.*?)<\/keywordGroup>/i ) {
        $rec{"KWD-GEN"} = $1;
        $rec{"KWD-GEN"} =~ s/<keyword\sonlyChannels.*?>//g;
        $rec{"KWD-GEN"} =~ s/<keyword\sxml:id=.*?>//g;
        $rec{"KWD-GEN"} =~ s/<\/keyword>/, /ig;
        $rec{"KWD-GEN"} =~ s/^\s|\s$//g;
        $rec{"KWD-GEN"} =~ s/\,$//g;
            $rec{"KWD-GEN"} =~ s/<fi>(.*?)<\/fi>/$1/g;
        } elsif( $line =~ /<keywordGroup.*?>(.*?)<\/keywordGroup>/i ) {
        $rec{"KWD-GEN"} = $1;
        $rec{"KWD-GEN"} =~ s/<keyword\sonlyChannels.*?>//g;
        $rec{"KWD-GEN"} =~ s/<keyword\sxml:id=.*?>//g;
        $rec{"KWD-GEN"} =~ s/<\/keyword>/, /ig;
        $rec{"KWD-GEN"} =~ s/^\s|\s$//g;
        $rec{"KWD-GEN"} =~ s/\,$//g;
            $rec{"KWD-GEN"} =~ s/<fi>(.*?)<\/fi>/$1/g;
    }
    return;
}

##############################################################################
# Name of Module:  &ComputeDerivedFields() - compute journal name and other
#                  derived tags.
#
# Description:     This routine computes additional elements of the %rec array
#
# Inputs:          %rec -    the global array containing the data read in from
#                            the last bib record.
#
# Output:          Adds the following new elements to the %rec array:
#                      MNEMONIC   DESCRIPTION
#                      --------   -----------------------------------------
#                        JNL      journal name (with volume, page)
#                        ORG      Origin ("WILEY")
#                        ENT      Entry date of paper (today's date)
#
# Overview:
#
##############################################################################

sub ComputeDerivedFields {

    my($fpage,$lpage);

    $rec{"P"} =~ s/l/L/g;
    if($ispage) {
        if( $rec{"P"} =~ /(\d\d)(\d\d\d)/ ) {
             $fpage = "$1,$2";
        } else {
             $fpage = $rec{"P"};
        }
        if( $rec{"L"} =~ /(\d\d)(\d\d\d)/ ) {
             $lpage = "$1,$2";
        } else {
             $lpage = $rec{"L"};
        }
        $rec{"JNL"} = ($rec{"J"} . ", Volume " . $rec{"V"} . ", Issue " . $rec{"I"} . ", pp. " . $fpage . "-" . $lpage );
        $ispage = 0;
    } else {
        $fpage = $rec{"DOI"};
        $fpage =~ s/.*\///g;
        $rec{"JNL"} = ($rec{"J"} . ", Volume " . $rec{"V"} . ", Issue " . $rec{"I"} . ", article id. e" . $fpage );
    }
    # Assign origin
    $rec{"ORG"} = "WILEY";                 # origin = WILEY

    # Assign entry date
    $rec{"PUB"} = "$rec{'PMO'}\/$rec{'Y'}";
}


##############################################################################
# Name of Module:  &ReadCites() - reads in citations
#
# Inputs:          string with all citations
#
# Output:          single citations to be written out to REF file
#
##############################################################################

sub ReadCites {

    my($citestring) = $_[0];
    my(@cites,$c);

    $REF_FILE = "$REFDIR/iss$rec{'I'}.wiley2.xml";

    print STDERR "Using $REF_FILE for references\n";

    open(REFFILE,">> $REF_FILE") || die "Couldn't open output file $REF_FILE$!\n";

#   $citestring = ADS::Abstracts::Entities::Encoder->new(Encoding => "UTF-8", Format   => "Entity")->recode($citestring);
    $citestring = &ADSrecode($citestring);
    $citestring = &FixAccents($citestring);

    $citestring =~ s/.*?<bibliography\sxml:id=\"\S+\"\sstyle=\"\S+\">//ig;
    $citestring =~ s/.*?<title\stype=\"main\">REFERENCES<\/title>//ig;
    @cites = split(/<\/bib>/,$citestring);
    $cites[0] =~ s/.*<\/p>//ig;

    if($#cites > 0 ) {
        print REFFILE "<ADSBIBCODE>$rec{'BIB'}<\/ADSBIBCODE>\n";
    }
    for $c(0..$#cites-1) {
        $cites[$c] =~ s/^\s*|\s*$//g;
        print REFFILE "$cites[$c]<\/bib>\n"
    }
    close(REFFILE);
}


##############################################################################
# Name of Module:  &ParseArgs()
#
# Function:        Parse command line arguments to program
#
# Input:           Global @ARGV array
#
# Output:          Sets global variables:
##############################################################################

sub ParseArgs {

    my($switch,$arg);

    if (@ARGV == 0) {
    &Usage;
    }
    while (substr($ARGV[0],0,1) eq "-") {
    if (@ARGV == 0) {
        &Usage;
    }
    if (! defined($switch = shift(@ARGV))) {
        &Usage;
    } else {
        &Usage;
    }
    }
}


sub Usage {
    print STDERR<<"ENDOFUSAGE";
Usage: wiley.pl [options ... ] infile1 [infile2 ... ]
where options include:
ENDOFUSAGE
    exit(1);
}


################################################################################
# Name of Module:  &WriteOutput() - writes out tagged output
#
# Output:          Writes output to file
#
##############################################################################

sub WriteOutput {

    if($rec{"OPEN"}) {
        print STDOUT "%R $rec{'BIB'}\n%T $rec{'XTL'}\n%A $rec{'AUT'}\n%F $rec{'AFF'}\n%J $rec{'JNL'}\n%D $rec{'PUB'}\n%C $rec{'COP'}\n%K $rec{'KWD-GEN'}\n%I DOI: $rec{'DOI'}; OPEN: \n%B $rec{'ABS'}\n\n";
    } else {
        print STDOUT "%R $rec{'BIB'}\n%T $rec{'XTL'}\n%A $rec{'AUT'}\n%F $rec{'AFF'}\n%J $rec{'JNL'}\n%D $rec{'PUB'}\n%C $rec{'COP'}\n%K $rec{'KWD-GEN'}\n%I DOI: $rec{'DOI'}\n%B $rec{'ABS'}\n\n";
    }
#   print STDOUT "%R $rec{'BIB'}\n%K $rec{'KWD-GEN'}\n\n";

}


##############################################################################
# Name of Module:  &AssignBibcode() - assign bibcode to record
#
# Description:     Assigns bibcode to record.
#
# Inputs:          $rec{"P"}, $rec{"V"}, and $rec{"AUT"}
#
# Output:          $rec{"BIB"}
#
##############################################################################


sub AssignBibcode {
    my($jnl,$pad1,$pad2,$finit,$pag,$iss);

    $pad1 = '.' x (4 - length($rec{"V"}));
    $finit = substr($rec{"AUT"},0,1);
    if( $finit eq "&" ) {
        $finit = substr($rec{"AUT"},1,1);
    }
    if(! $rec{"AUT"}) {
        $finit = ".";
    }
    $finit =~ tr/a-z/A-Z/;

    $rec{"P"} =~ s/^GB/\./g;
    $rec{"P"} =~ s/^PA/\./g;
    $rec{"P"} =~ s/^R[GS]/\./g;
    $rec{"P"} =~ s/^[ABCDEFGLMQSW]0?/\./g;
    $rec{"P"} =~ s/^TC/\./g;
    $rec{"P"} =~ s/\,//g;
    if(length($rec{'P'}) == 6 ) {
        $rec{"P"} =~ s/^\.//g;
    }
    # letters
    if( $rec{"P"} =~ /L$/i ) {
        $rec{"P"} = substr($rec{"P"},0,length($rec{"P"}-1));
        $pad2 = '.' x (4 - length($rec{"P"}));
        $rec{"BIB"} = ( $rec{"Y"} . $rec{'BS'} . $pad1 . $rec{"V"} . "L" . $pad2 . $rec{"P"} . $finit );
    } else {
        $pad2 = '.' x (5 - length($rec{"P"}));
        $rec{"BIB"} = ( $rec{"Y"} . $rec{'BS'} . $pad1 . $rec{"V"} . $pad2 . $rec{"P"} . $finit );
    }

        # write DOI
    print $doi "$rec{'BIB'}\t$rec{'DOI'}\n";
}


##############################################################################
# Name of Module:  &Escape()
#
# Function:        Escape < and > for all cases except
#                  SUB, SUP, PRE, BR, URL, EMAIL, ASTROBJ
#
# Input:           unescaped string
#
# Output:          escaped string
#
##############################################################################

sub Escape {

    my($string) = $_[0];

    $string =~ s/</\&lt\;/g;
    $string =~ s/>/\&gt\;/g;

    $string =~ s/\&lt\;SUB\&gt\;/<SUB>/ig;
    $string =~ s/\&lt\;\/SUB\&gt\;/<\/SUB>/ig;
    $string =~ s/\&lt\;SUP\&gt\;/<SUP>/ig;
    $string =~ s/\&lt\;\/SUP\&gt\;/<\/SUP>/ig;
    $string =~ s/\&lt\;PRE\&gt\;/<PRE>/ig;
    $string =~ s/\&lt\;\/PRE\&gt\;/<\/PRE>/ig;
    $string =~ s/\&lt\;BR\&gt\;/<BR>/ig;
    $string =~ s/\&lt\;\/BR\&gt\;/<\/BR>/ig;
    $string =~ s/\&lt\;URL\&gt\;/<URL>/ig;
    $string =~ s/\&lt\;\/URL\&gt\;/<\/URL>/ig;
    $string =~ s/\&lt\;EMAIL\&gt\;/<EMAIL>/ig;
    $string =~ s/\&lt\;\/EMAIL\&gt\;/<\/EMAIL>/ig;
    $string =~ s/\&lt\;ASTROBJ\&gt\;/<ASTROBJ>/ig;
    $string =~ s/\&lt\;\/ASTROBJ\&gt\;/<\/ASTROBJ>/ig;
    if( $string =~ s/\&lt\;A\sHREF/<A HREF/ig ) {
        $string =~ s/\&lt\;\/A\&gt\;/<\/A>/ig;
        $string =~ s/\&gt\;/>/ig;
    }

    $string =~ s/\&([^\w])/\&amp\;$1/g;

    return($string);
}


##############################################################################
# Name of Module:  &DeSGML()
#
# Function:        Pre-process files to eliminate SGML-specific tags.
#
# Input:           line to be processed
#
# Output:          line without SGML symbols
#
##############################################################################

sub DeSGML {
    my($inline) = $_[0];
    my($outline) = "";
    my(@line,$i,$line,$code,$new,@mult,$j);

    # rewrite SGML entities
    @line = split(/\s+/,$inline);
    for $i(0..$#line) {
        if( $line[$i] =~ /\&[^\;]+\;/ ) {
            @mult = split("\;",$line[$i]);
            for $j(0..$#mult) {
                if( $mult[$j] =~ /\&(.+)$/ ) {
                    $code = $1;
                    if( $entities{$code} ) {
                        $mult[$j] =~ s/\&(.+)$/$entities{$code}/;
                    } else {
                        $mult[$j] = $mult[$j] . "\;";
                    }
                }
                if( $j != 0 ) {
                    $new = $new . $mult[$j];
                } else {
                    $new = $mult[$j];
                }
            }
            $line[$i] = $new;
        }
        if( $line ) {
            $line = join(" ",$line,$line[$i]);
        } else {
            $line = $line[$i];
        }
        $new = "";
    }
    $outline = $line;

    # now remove other SGML tags
    $outline =~ s/<e1>([^<]*)<\/e1>/$1/ig;
    $outline =~ s/<e2>([^<]*)<\/e2>/$1/ig;
    $outline =~ s/<e3>([^<]*)<\/e3>/$1/ig;
    $outline =~ s/<e4>([^<]*)<\/e4>/$1/ig;
    $outline =~ s/<e5>([^<]*)<\/e5>/$1/ig;
    $outline =~ s/<e1>//ig;
    $outline =~ s/<\/e1>//ig;
    $outline =~ s/<x>//ig;
    $outline =~ s/<\/x>//ig;
    $outline =~ s/<I>([^<]*)<\/I>/$1/ig;
    $outline =~ s/<IT>([^<]*)<\/IT>/$1/ig;
    $outline =~ s/<BF>([^<]*)<\/BF>/$1/ig;
    $outline =~ s/<B>([^<]*)<\/B>/$1/ig;
    $outline =~ s/<SS>([^<]*)<\/SS>/$1/ig;
    $outline =~ s/<UNL>([^<]*)<\/UNL>/$1/ig;
    $outline =~ s/<RM>([^<]*)<\/RM>/$1/ig;
    $outline =~ s/<sc>([^<]*)<\/sc>/uc($1)/ige;
    $outline =~ s/<scaps>([^<]*)<\/scaps>/uc($1)/ige;
    $outline =~ s/<smallcapitals>([^<]*)<\/smallcapitals>/uc($1)/ige;
    $outline =~ s/<AC>([^<]*)<\/AC>/$1/ig;
    $outline =~ s/<A>([^<]*)<UA>([^<]*)<\/UA><\/A>/$1$2/ig;
#   $outline =~ s/<SUP>([^<]*)<\/SUP>/^$1/ig;
#   $outline =~ s/<SUB>([^<]*)<\/SUB>/\_$1/ig;
    $outline =~ s/<INF>([^<]*)<\/INF>/<SUB>$1<\/SUB>/ig;
    $outline =~ s/<SBT>([^<]*)<\/SBT>/$1/ig;
    $outline =~ s/<F\sDCN\s\=\s\"GEO.FORM\">//ig;
    $outline =~ s/<\/F>//ig;
    $outline =~ s/<ZW>//ig;
    $outline =~ s/<fc>//ig;
    $outline =~ s/<\/fc>//ig;
    $outline =~ s/<I>//ig;
    $outline =~ s/<\/I>//ig;
    $outline =~ s/<HSP\sSP\s\=\"\d+\.*\d*\">//ig;
#   $outline =~ s/<\/p>\s*<p>/\n\n/ig;
    $outline =~ s/<sup>(.*?)<\/sup>/<SUP>$1<\/SUP>/ig;
    $outline =~ s/<sub>(.*?)<\/sub>/<SUB>$1<\/SUB>/ig;
    $outline =~ s/\-\-/\-/g;
    $outline =~ s/<mover>//ig;
    $outline =~ s/<mover\saccent=\"\S+\">//ig;
    $outline =~ s/<\/mover>//ig;
    $outline =~ s/<mo\sstretchy=\"\S+\"//g;
    $outline =~ s/<mrow>//ig;
    $outline =~ s/<\/mrow>//ig;
    $outline =~ s/<mi.*?>//g;
    $outline =~ s/<\/mi>//g;
    $outline =~ s/<mn>//g;
    $outline =~ s/<\/mn>//g;
    $outline =~ s/<mo>//g;
    $outline =~ s/<\/mo>//g;
    $outline =~ s/<mtext>//g;
    $outline =~ s/<\/mtext>//g;
    $outline =~ s/<math xmlns=\".*?>//g;
    $outline =~ s/<mspace.*?>/ /g;
    $outline =~ s/<msub>(.*?)<\/msub>/$1<SUB>$2<\/SUB>/ig;
    $outline =~ s/<msup>(.*?)<\/msup>/$1<SUP>$2<\/SUP>/ig;
    $outline =~ s/<msubsup>//ig;
    $outline =~ s/<\/msubsup>//ig;
    $outline =~ s/<\/simplemath>//ig;
    $outline =~ s/\&bsl00064\;(\d+)/.$1\&deg\;/g;
    if( $outline =~ s/<alternativemath\stype=\"latex2e\"><tex>(.*?)<\/alternativemath>/$1/ig ) {
        $outline =~ s/<\/tex>>*//ig;
        $outline =~ s/<\!\[CDATA\[//g;
        $outline =~ s/\!\[CDATA\[//g;
        $outline =~ s/\]\]>//g;
        $outline =~ s/\]\]//g;
        $outline =~ s/\\mathit{(.*?)}/$1/ig;
        $outline =~ s/\\mathrm{(.*?)}/$1/ig;
        $outline =~ s/<alternativemath\stype=\"mathml\">.*?<\/alternativemath>//ig;
    }
    if( $outline =~ s/<alternativemath\stype=\"mathml\"><mo>//ig ) {
        $outline =~ s/<\/alternativemath>//ig;
        $outline =~ s/<\/mo>//ig;
        $outline =~ s/<mi\sfontstyle=\"italic\">//g;
        $outline =~ s/<\/mi>//ig;
    }
    $outline =~ s/<math>//ig;
    $outline =~ s/<\/math>//ig;
    $outline =~ s/<formula\sformat=\"inline\">//ig;
    $outline =~ s/<formula[^>]>//ig;
    $outline =~ s/<\/formula>//ig;
    $outline =~ s/<warning>.*?<\/warning>//g;
    $outline =~ s/<span\scssStyle.*?>//g;
    $outline =~ s/<\/span>//g;

    # formula fix
    if( $outline =~ s/<\!\-<tex>.*?<\/tex>//g ) {
        $outline =~ s/\->//g;
    }
    $outline =~ s/<file\sname=\"\S+\"\stype=\"gif\"\/>//g;
    $outline =~ s/\^{(.*?)}/<SUP>$1<\/SUP>/ig;
    $outline =~ s/_{(.*?)}/<SUB>$1<\/SUB>/ig;

    # and SGML entities
    $outline =~ s/\&rsquo\;/\'/ig;
    $outline =~ s/\&lsquo\;/\'/ig;
    $outline =~ s/\&rdquo\;/\"/ig;
    $outline =~ s/\&ldquo\;/\"/ig;
    $outline =~ s/\&ndash\;/-/ig;
    $outline =~ s/\&macr\;//ig;
    $outline =~ s/\&thinsp\;//ig;
    $outline =~ s/\&hairsp\;//ig;
    $outline =~ s/\&blank\;/ /ig;
    $outline =~ s/\&nbsp\;//ig;
    $outline =~ s/\&quest\;/\?/ig;
    $outline =~ s/\&z\.scrpt\;//ig;
    $outline =~ s/\&ndash\;/-/ig;
    $outline =~ s/\&hyphen\;/-/ig;
    $outline =~ s/\&minus\;/-/ig;
    $outline =~ s/\&emsp\;/ /ig;
    $outline =~ s/\&ensp\;/ /ig;
    $outline =~ s/\&nbsp\;/ /ig;
    $outline =~ s/\&rsquo\;/\'/ig;
    $outline =~ s/\&lsquo\;/\'/ig;
    $outline =~ s/\&rdquo\;/\"/ig;
    $outline =~ s/\&ldquo\;/\"/ig;

    # rewrite accents
    $outline =~ s/([A-Za-z])\&(acute\;)/\&$1$2/ig;
    $outline =~ s/([A-Za-z])\&(breve\;)/\&$1$2/ig;
    $outline =~ s/([A-Za-z])\&(grave\;)/\&$1$2/ig;
    $outline =~ s/([A-Za-z])\&(circ\;)/\&$1$2/ig;
    $outline =~ s/([A-Za-z])\&(caron\;)/\&$1$2/ig;
    $outline =~ s/([A-Za-z])\&(dblac\;)/\&$1$2/ig;
    $outline =~ s/([A-Za-z])\&(dot\;)/\&$1$2/ig;
    $outline =~ s/([A-Za-z])\&(macr\;)/\&$1$2/ig;
    $outline =~ s/([A-Za-z])\&(midot\;)/\&$1$2/ig;
    $outline =~ s/([A-Za-z])\&(nodot\;)/\&$1$2/ig;
    $outline =~ s/([A-Za-z])\&(ring\;)/\&$1$2/ig;
    $outline =~ s/([A-Za-z])\&(slash\;)/\&$1$2/ig;
#   $outline =~ s/([A-Za-z])*\&(szlig\;)/\&$1$2/ig;
#   $outline =~ s/([A-Za-z])*\&(aelig\;)/\&$1$2/ig;
    $outline =~ s/([A-Za-z])\&(tilde\;)/\&$1$2/ig;
    $outline =~ s/([A-Za-z])\&(uml\;)/\&$1$2/ig;

    return $outline;
}



##############################################################################
# Name of Module:  &FixAccents()
#
# Function:        Pre-process files to eliminate high-bit characters
#
# Input:           line to be processed
#
# Output:          line without highbit
#
##############################################################################

sub FixAccents {

    my($inline) = $_[0];
    my($outline) = "";

    $inline =~ s/\s*\&acirc\;\.*\&\#128\;\&\#144\;/-/ig;
    $inline =~ s/\s*\&acirc\;\.*\&\#128\;\&\#147\;/-/ig;
    $inline =~ s/\&acirc\;\.*\&\#128\;\&nacute\;/-/ig;
    $inline =~ s/\&acirc\;\&\#128\;\&\#153\;/\'/ig;
    $inline =~ s/\&acirc\;\&\#128\;\&\#156\;/\"/ig;
    $inline =~ s/\&acirc\;\&\#128\;\&\#157\;/\"/ig;

    $outline = $inline;
    return $outline;
}



sub ADSrecode {
    my $string = shift;
        return $entity_enc->recode($string);
    }
