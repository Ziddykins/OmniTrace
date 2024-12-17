#!/usr/bin/env perl
use strict;
use warnings;
use English;

use Data::Dumper;
use JSON qw(encode_json);

#<Placemark>
#                <name>(no SSID)</name>
#                <open>1</open>
#                <description>Network ID: FA:9B:6E:C4:B7:02
#Encryption: WPA2
#Time: 2023-11-04T04:31:54.000-07:00
#Signal: -61.0
#Accuracy: 1.9
#Type: WIFI</description>
#                <styleUrl>#highConfidence</styleUrl>
#                <Point>
#                    <coordinates>-76.50110626,44.24169922</coordinates>
#                </Point>
#            </Placemark>##

my %data;
opendir my $dh, './kmls/' or die "can't do it: $!\n";

while(readdir $dh) {
    my $file = $_;
    my $slurped;

    next if $file =~ /^\.\.?/;

    print "opening file: $file\n";
    open my $fh, '<', "kmls/$file" or die "nop! $!\n";
    
    {
        local $/;
        $slurped = <$fh>;
        $slurped =~ s/[\t\r\n ]+//g;
    }

    close $fh;

    my @matches = $slurped =~ /<Folder>(.*?)<\/Folder>/g;
    
    foreach my $folder (@matches) {
        my $type = $1 if $folder =~ /<name>(.*?)</;
        my @placemarks = $folder =~ /<Placemark>(.*?)<\/Placemark>/g;
        $type = $1 if $type =~ /(Cell|Wifi|Bluetooth)/;

        foreach my $placemark (@placemarks) {
            my $name = $1 if $placemark =~ /<name>(.*?)</;
            my $desc = $1 if $placemark =~ /<description>(.*?)<\/Point/;
            my ($netid, $ttype);

            if ($desc =~ /Net.*?ID:(?<netid>.*?)(?:Encryption:(?<enc>.*?))?Time:(?<time>.*?)Signal:(?<sig>.*?)Accuracy:(?<acc>.*?)Type(?<type>.*?)(?:Attributes:(?<attr>.*?))?<\/d.*?coordinates>(?<crd>.*?)</g) {
                my ($long, $lat) = split ',', $+{crd};
                ($netid, $ttype) = ($+{netid}, $+{type});
                $ttype =~ s/://g;

                $data{$type}{$ttype}{$name}{$netid}{netid}        = $+{netid};
                $data{$type}{$ttype}{$name}{$netid}{time}         = $+{time};
                $data{$type}{$ttype}{$name}{$netid}{signal}       = $+{sig};
                $data{$type}{$ttype}{$name}{$netid}{accuracy}     = $+{acc};
                $data{$type}{$ttype}{$name}{$netid}{type}         = $+{type};
                $data{$type}{$ttype}{$name}{$netid}{coords}{long} = $long;
                $data{$type}{$ttype}{$name}{$netid}{coords}{lat}  = $lat;
            } else {
                die "duhhhhwhooops! no match: $desc\n";
            }

            if ($type eq 'Bluetooth') {
                $data{$type}{$ttype}{$name}{$netid}{attributes} = $+{attr};
            } elsif ($type eq 'Wifi') {
                $data{$type}{$ttype}{$name}{$netid}{encryption} = $+{enc};
            }
        }
    }
}

open my $jfh, '>', 'json.txt';
print $jfh encode_json(\%data);
close $jfh;

open my $fh, '>', 'omg.txt';
print $fh Dumper(%data);
close $fh;

closedir $dh;

print "Done :D\n";