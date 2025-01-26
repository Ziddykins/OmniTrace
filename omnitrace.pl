#!/usr/bin/env perl
use strict;
use warnings;
use English;
use feature 'state';
use utf8;

use File::stat;
use File::Find;
use Data::Dumper;
use XML::LibXML;
use Geo::GDAL::FFI;
use JSON qw(encode_json);
use Getopt::Long qw(:config bundling);
use Cwd;

use vars qw/*name *dir *prune/;
*name   = *File::Find::name;
*dir    = *File::Find::dir;
*prune = *File::Find::prune;
sub wanted;

our $VERSION = 1.2;

# Main hash for storing data from parsed files
my %data;

my $parser = XML::LibXML->new();

# Getops
my ($folder, $file_path, $destination, $file, $all);
my ($ignore, $combine, $recursive);
my ($sql_host, $sql_pass, $sql_db, $sql_tbl, $sql_user, $sql_port);
my $output_type;

# Array populated when searching for files
my @found_files;

# Current directory
my $cd;

# Sets the XML parser namespace - defaults to kml
my $namespace = 'http://www.opengis.net/kml/2.2';

GetOptions(
    "a|all"        => \$all,
    "o|otype=s"      => \$output_type,
    "f|file=s"       => \$file,
    "F|folder=s"     => \$folder,
    "d|dest=s"       => \$destination,
    "h|help"         => sub { help() },
    "v|version"      => sub { print $VERSION; },
    "i|ignore|force" => \$ignore,
    "c|combine"      => \$combine,
    "r|recursive"    => \$recursive,
    "n|namespace"    => \$namespace,
    "H|host=s"       => \$sql_host,
    "P|password=s"   => \$sql_pass,
    "T|table=s"      => \$sql_tbl,
    "D|database=s"   => \$sql_db,
    "R|port=s"       => \$sql_port,

);

exit -1 if argument_checks();
$namespace = '/kml';
find_files($folder, $recursive);

foreach my $file (@found_files) {
    my $type = $1 if $file =~ /\.(kml|gpkg|csv|json)$/i;

    if ($type eq 'kml') {
        parse_kml($file);
    } elsif ($type eq 'gpkg') {
        parse_gpkg($file);
    } elsif ($type eq 'json') {
        parse_json($file);
    } elsif ($type eq 'csv') {
        parse_csv($file);
    } else {
        print "Unsupported file type: $type\n";
        next;
    }
}

sub parse_kml {
    my $file = $_[0];
    my $xml = $parser->parse_file($file) or die "Can't do it: $!\n";
    my $xpc = XML::LibXML::XPathContext->new($xml);
    $xpc->registerNs('kml', $namespace);

    foreach my $folder ($xpc->findnodes('//kml:Folder')) {
        my $type;
        if ($xpc->findvalue("./kml:name", $folder) =~ /(Cell|Wifi|Bluetooth)/) {
            $type = $1;
            print "Type: $type found\n";
        }
        
        my @placemarks = $xpc->findnodes("./kml:Placemark", $folder);

        foreach my $placemark (@placemarks) {
            my $name   = $xpc->findvalue("./kml:name", $placemark);
            my $desc   = $xpc->findvalue("./kml:description", $placemark);
            my $coords = $xpc->findvalue("./kml:Point/kml:coordinates", $placemark);
            my ($netid, $ttype);
            my ($long, $lat) = split ',', $coords;

            $netid = $1 if $desc =~ /Net.*?ID: (.*?)[\r\n]/;
            $ttype = $1 if $desc =~ /Type: (.*)/;
            chomp $ttype;

            $data{$type}{$ttype}{$name}{$netid}{encryption} = $1 if $desc =~ /Encryption: (.*?)[\r\n]/;
            $data{$type}{$ttype}{$name}{$netid}{time}       = $1 if $desc =~ /Time: (.*?)[\r\n]/;
            $data{$type}{$ttype}{$name}{$netid}{signal}     = $1 if $desc =~ /Signal: (.*?)[\r\n]/;
            $data{$type}{$ttype}{$name}{$netid}{accuracy}   = $1 if $desc =~ /Accuracy: (.*?)[\r\n]/;
            chomp($data{$type}{$ttype}{$name}{$netid}{attributes} = $1) if $desc =~ /Attributes: (.*)/;
            $data{$type}{$ttype}{$name}{$netid}{coords}{longitude} = $long;
            $data{$type}{$ttype}{$name}{$netid}{coords}{latitude}  = $lat;
        }
    }
}

sub find_files {
    my ($path, $recursive) = @_;

    if ($recursive) {
        File::Find::find({wanted => \&wanted}, $path);
    } else {
        opendir my $dh, $folder or die "can't do it: $!\n";

        while (readdir $dh) {
            my $file = $_;
            my $full_path = "$folder/$file";
            
            my $file_size = 0;
            $full_path =~ s/[\r\n]+//g;

            next if !-f $full_path;
            next if $file =~ /example$/;

            $file_size = -s $full_path;
            next if !$file_size;

            next if $full_path !~ /\.(gpkg|kml|csv)$/i;

            push @found_files, $full_path;
        }
        closedir $dh;
    }
}

sub wanted {
    /^.*\.(kml|csv|gpkg)$/si && push @found_files, $name;
    #    /^.*\.$output_type\z/s && push @found_files, $name;
    
}

sub help {
    print_banner();

    my $help = <<HELP;
        $0 - version $VERSION\n
        - Program Options
            [ -a | --all              ] - If specified, will find all accepted file types
            [ -o | --otype  <#>       ] - If specified, sets the output type; otherwise,
                                          the output type will be determined by the file
                                          extension; if not supported, csv will be the
                                          output
            [ -f | --file   <file>    ] - If specified, only process the specified file
            [ -F | --folder <path>    ] - If specified, only process the specified folder
            [ -d | --dest   <path>    ] - If specified, the output will be saved to the
                                          specified folder
            [ -i | --ignore           ] - Ignores parsing errors and continues processing the file
               `-> --force
            [ -c | --combined         ] - Combine all output files into a single file
            [ -r | --recursive        ] - Process the specified folder recursively

        - SQL options
            [ -E | --sql-enable        ] - If set, and below information is supplied, this will
                                           gather all the found files and populate the database
                                           with the results from the main %data hash
            [ -H | --host  [hostname]  ] - SQL hostname, defaults to 'localhost'
            [ -U | --user  [username]  ] - SQL username, defaults to 'root'
            [ -P | --pass  [password]  ] - SQL password, defaults to an empty password
            [ -D | --db    <database>  ] - SQL database, no default, required
            [ -T | --table <table>     ] - SQL table, no default, required
            [ -R | --port  [port]      ] - SQL port, defaults to '3306'
        
        - Other commands
            [ -h | --help             ] - Display this help message
            [ -v | --version          ] - Display the version number

        - Input/Output
            Output Types:       Input Types:
              1 - KML             1 - GPKG 
              2 - CSV             2 - CSV
              3 - JSON            3 - KML

            Examples:
                $0 --otype json --folder kmls/ --dest output/ --recursive --combined

HELP
    print $help;
    exit 0;
}

sub argument_checks {
    if ($folder && $file_path) {
        print "Error: You can't specify both a folder and a file.\n";
        return -1;
    }

    if ($folder && !-d $folder) {
        print "Error: The specified folder does not exist.\n";
        return -1;
    }

    if ($file_path && !-e $file_path) {
        print "Error: The specified file does not exist.\n";
        return -1;
    }

    if ($destination && !-d $destination) {
        print "Error: The specified destination folder does not exist.\n";
        return -1;
    }

    if ($output_type && $output_type !~ /\.?(kml|csv|json)$/) {
        print "Error: The specified output type is not valid\n";
        print "Valid: kml | csv | json\n";
        return -1;
    }

    if ($file && $all || $folder && !$all) {
        print STDERR "Cannot specify --file with --all, or --folder without --all\n";
    }

    return 0;
}

=head2 combine_files

  combine_files(\@file_list, $output_type);

Takes an array ref which contains a list of paths to  files,
parses them, and combines them all into a hash.

=item $firecracker->boom()

This noisily detonates the firecracker object.

=cut

=cut
sub combine_files {
    my (@file_list, $output_type) = @_;
    my %temp;

    foreach my $file (@file_list) {
        open my $fh, '<', $file or die "Can't open $file: $!";
        my $data = do { local $/; <$fh> };
        my @tmp = split '.', $file;
        my $ext = $tmp[-1];

        if ($ext eq 'kml') {
            # do that
        } elsif ($ext eq 'json') {
            my %temp = decode_json($data);
            my $href = merge_hashes(\%data, \%temp);
            %data = %$href;
        } elsif ($ext eq 'csv') {
            my (@columns, @values);
            my $first_line;
            my %temp;
            my $parsed_columns = 0;

            foreach my $line (split /[\r\n]/, $data) {
                if (!$parsed_columns++) {
                    @columns = split /,/, $line;
                }

                @values = split /,/, $line;

                for (my $i=0; $i<@values; $i++) {
                    $temp{$columns[$i]}{value} = $values[$i];
                }
            }

            my $href = merge_hashes(\%data, \%temp);
            %data = %$href;
        } elsif ($ext eq 'geojson') {
            # do that
        }
    }
}

=head2 merge_hashes: Arguments
  merge_hashes($href1, $href2)
Merges two multidimensional hashes recursively. If a value is found in the 2nd hash
which isn't present in the first hash, you have the option to

=head2 merge_hashes: returns

A reference to the merged hash.

=cut
sub merge_hashes {
    print "Entering function\n";
    my ($hash1, $hash2) = @_;
    state $skip_all    = 0;
    state $ow_all      = 0;
    state $skip_count  = 0;
    state $ow_count    = 0;
    state $merge_count = 0;
    state $nested      = 0;

    foreach my $key (keys %$hash2) {
        if (exists $hash1->{$key} && ref $hash1->{$key} eq 'HASH' && ref $hash2->{$key} eq 'HASH') {
            $nested++;
            $hash1->{$key} = merge_hashes($hash1->{$key}, $hash2->{$key});
        } elsif (exists $hash1->{$key} && $hash1->{$key} ne $hash2->{$key} && !$ignore) {
            if (!$skip_all && !$ow_all) {
                print "Warning: Key '$key' already exists in the hash and has a different value.\n";
                print "1. Keep current (" . $hash1->{$key} . ") (skip)\n";
                print "2. Replace with (" . $hash2->{$key} . ") (overwrite)\n";
                print "3. Skip All\n";
                print "4. Overwrite All\n";
                print "0. Cancel merge\n";
                chomp(my $choice = <STDIN>);

                if ($choice eq '1') {
                    $skip_count++;
                } elsif ($choice eq '2') {
                    $hash1->{$key} = $hash2->{$key};
                    $ow_count++;
                } elsif ($choice eq '3') {
                    $skip_all = 1;
                    $skip_count++;
                } elsif ($choice eq '4') {
                    $ow_all = 1;
                    $ow_count++;
                } else {
                    return;
                }
            } else {
                if ($ow_all) {
                    $hash1->{$key} = $hash2->{$key};
                    $ow_count++;
                } else {
                    $skip_count++;
                }
            }
        } else {
            $hash1->{$key} = $hash2->{$key};
            $merge_count++;
        }
    }

    printf "%d entries merged, %d entries overwritten and %d entries skipped\n", $merge_count, $ow_count, $skip_count;

    if (!$nested) {
        $ow_count = $merge_count = $skip_count = 0;
        $skip_all = $ow_all = 0;
    }

    $nested--;
    printf "%d %d %d %d %d %d\n", $skip_all, $ow_all, $skip_count, $ow_count, $merge_count, $nested;
    return $hash1;
}

sub parse_gpkg {
    my ($file) = @_;
    my @features;
    my %new_data;

    my %type_map = (
        'LTE_MESSAGE' => {
            'type' => 'Cell',
            'ttype' => 'LTE'
        },
        'NR_MESSAGE' => {
            'type' => 'Cell',
            'ttype' => 'NR'
        },
        'GNSS_MESSAGE' => {
            'type' => 'Cell',
            'ttype' => 'GNSS'
        },
        'PHONE_STATE_MESSAGE' => {
            'type' => 'Cell',
            'ttype' => 'PhoneState'
        },
        '80211_BEACON_MESSAGE' => {
            'type' => 'Wifi',
            'ttype' => 'WIFI',
        },
        'BLUETOOTH_MESSAGE' => {
            'type' => 'Bluetooth'
            # ttype = ble/le, etc
        }        
    );    
    
    # Open dataset
    my $dataset = Geo::GDAL::FFI::Open($file);
    die "Could not open GPKG: $file" unless $dataset;

    my $layer_count = $dataset->GetLayerCount;

    for (my $i=0; $i<$layer_count-1; $i++) {
        my $layer = $dataset->GetLayer($i);
        my $definition = $layer->GetDefn;
        my $last_type = "none";
        while (my $feature = $layer->GetNextFeature) {
            my $featdfn      = $feature->GetDefn;
            my $field_schema = $featdfn->GetSchema;
            my @fields_href  = @{$field_schema->{Fields}};            
            my $geometry     = $feature->GetGeomField;
            my $gpkg_type = $field_schema->{Name};
            my @fields;
#BLUETOOTH_MESSAGE
#80211_BEACON_MESSAGE

            foreach my $field (@fields_href) {
                push @fields, $field->{Name};
            }

            if ($last_type ne $gpkg_type) {
                $last_type = $gpkg_type;
                
                foreach my $field (@fields) {
                    #      cell/bt gsm,lte ap/dev macaddr
                    #$data{$type}{$ttype}{$name}{$netid}{encryption}
                    my $type  = $type_map{$gpkg_type}{type};
                    my $ttype = $type_map{$gpkg_type}{ttype};
                    my $mac;

                    if ($gpkg_type eq 'BLUETOOTH_MESSAGE') {
                        $mac = $feature->GetField('Source Address');
                    } elsif ($gpkg_type eq '80211_BEACON_MESSAGE') {
                        $mac = $feature->GetField('BSSID');
                        $ttype = $feature->GetField('Technology');
                    }

                    
                }
            } else {
                next;
            }            

            foreach my $field (@fields) {
                #$data{$type}{$ttype}{$name}{$netid}
                my $data = $feature->GetField($field);
                if ($data) {
                    print "Field: $field - Data: $data\n";
                }
            }

            if ($geometry) {
                my ($lon, $lat, $alt) = (0, 0, 0);

                if ($geometry->GetType eq 'Point25D') {
                    ($lon, $lat, $alt) = $geometry->GetPoint;
                }
            }
        }
    }
}

sub field_mapper {
    my %types = (
        'Bluetooth' => ''
    );
}

sub print_banner {
    print "\n\e[31m" . '
         .p888q.   `qpd8nqpn8qp    ,;pdoon    bq
        pn '."\e[34m~ ~\e[31m".' nq    89`  96 `e9   8e    bq     
        db  '."\e[34m ~\e[31m".' db    db   Y   db   db    db   jl 
        `8bp.qd8`   eaab     eaab  ab    .t,  lt;.' . "\n" .
"               88\n\e[33m" .
"         qb      \e[31m88                       " . "\e[0m\e[34m -jrb\e[33m" . '
       cadbpeq  `dp'."\e[31m88\e[33m".'qa  .p88p.   .p88p.    .p88p.
         8`      8`   J   ,.,db  db        db.,;dp
         8    ,  8      pd   pq  db    q;  lb
         lbmd9`  K      `BboodJ  `dbedd`   `lboad`' . "\e[0m\n";
}
