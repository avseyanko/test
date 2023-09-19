#!/usr/bin/perl

use strict;
use warnings;
use utf8;

#use lib './lib';

use Data::Dumper;
use Email::Address;
use utf8;

use DBI;

use constant BULK_SIZE => 100;


my @line_parts;
my $bulk_counter = 0;
my $counter = 0;

open(DATA, "<:encoding(UTF-8)", "out") or die "Cannot open source file out: $!";
my $dbh = DBI->connect("dbi:Pg:dbname=maillog;host=localhost", 'postgres', '1', {AutoCommit => 1}) or die "Cannot connect to database: ". DBI->errstr;

my $bulk = [];

while (my $next_line = <DATA>) {
    chomp($next_line);
    @line_parts = split(/\s/, $next_line, 6);
    my $new_message = {};
    $new_message->{'created'} = $line_parts[0] . ' ' . $line_parts[1];
    if ($line_parts[2]) {
        $new_message->{'int_id'} = $line_parts[2];
    }
    if (!$new_message->{'int_id'}) {
        # We have NOT NULL requirement for this field in both tables
        next;
    }

    $new_message->{'str'} = join(' ', grep {$_} @line_parts[2..5]);

    if ($line_parts[3] eq '<=') {
        if ( $line_parts[5] && ($line_parts[5] =~ /\sid=([^\s\n]+)/) ) {
            $new_message->{'id'} = $1;
        }
        next unless $new_message->{'id'};
        $new_message->{table_name} = 'message';
    } else {
        if ($new_message->{'str'}) {
            my @addresses = Email::Address->parse( $new_message->{'str'} );
            $new_message->{'address'} = $addresses[0]->address if $addresses[0];
        }
        $new_message->{table_name} = 'log';
    }

    push @$bulk, $new_message;

    ++$bulk_counter;
    ++$counter;

    if ($bulk_counter > BULK_SIZE) {
        my @log_messages = grep { $_->{table_name} eq 'log' } @$bulk;
        my @messages = grep { $_->{table_name} eq 'message' } @$bulk;

        $bulk = [];
        $bulk_counter = 0;
        do_insert(\@log_messages, \@messages, $dbh);
    }
}

# last part can be less then BULK_SIZE
if ($bulk && @$bulk) {
    my @log_messages = grep { $_->{table_name} eq 'log' } @$bulk;
    my @messages = grep { $_->{table_name} eq 'message' } @$bulk;
    do_insert(\@log_messages, \@messages, $dbh);
}

$dbh->disconnect;
close(DATA);

print "Everything is OK\n";
exit(0);


sub do_insert {
    my $log_messages = shift;
    my $messages = shift;
    my $dbh = shift;

    my $stmt_log = sprintf(
        'insert into public.log (created, int_id, str, address) values %s',
        join(',', map '(?,?,?,?)', (1..scalar @$log_messages))
    );

    my $stmt_messages = sprintf(
        'insert into public.message (created, id, int_id, str) values %s',
        join(',', map '(?,?,?,?)', (1..scalar @$messages))
    );

    my @logged = map { $_->{created}, $_->{int_id}, $_->{str}, $_->{address} } @{ $log_messages };
    if (scalar @logged) {
        my $result = $dbh->do($stmt_log, {}, @logged);
        print "Error inserting data: " . $dbh->errstr unless $result;
    }

    my @msg = map { $_->{created}, $_->{id}, $_->{int_id}, $_->{str} } @{ $messages };
    if (scalar @msg) {
        my $result = $dbh->do($stmt_messages, {}, @msg);
        print "Error inserting data: " . $dbh->errstr unless $result;
    }
}

1;
