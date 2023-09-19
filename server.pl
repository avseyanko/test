#!/usr/bin/perl

use strict;
use warnings;
use utf8;
use DBI;
use Encode;

{
    package MyServer;

    use HTTP::Server::Simple::CGI;
    use base qw(HTTP::Server::Simple::CGI);
    use URI::Escape;
    #use HTML::Entities; # Requires HTML::Parser, which requires XS # However server anyway requires HTML::Entities

    my %dispatch = (
        '/submit' => \&submit,
        '/' => \&root,
    );

    sub handle_request {
        my $self = shift;
        my $cgi  = shift;

        my $path = $cgi->path_info();
        my $handler = $dispatch{$path};

        if (ref($handler) eq "CODE") {
            print "HTTP/1.0 200 OK\r\n";
            $handler->($cgi);
        } else {
            print "HTTP/1.0 404 Not found\r\n";
            print "Content-type: text/html\n\n";
            print "<html>
                <head><title>404 - not found</title></head>
                <body>
                <p> Cannot find this page </p>
                </body>
                </html>
            ";
        }
    }

    sub submit {
        my $cgi  = shift;
        return if !ref $cgi;

#    my $who = HTML::Entities::decode_entities($cgi->param('search')); # Requires XS, so - skipped
        my $who = $cgi->param('search');
        $who = URI::Escape::uri_unescape($who);
        $who = Encode::decode_utf8($who);
        $who = '%' . lc($who) . '%';

        # Wrong solution, connect to DB for every request. However, should work
        my $dbh = DBI->connect("dbi:Pg:dbname=maillog;host=localhost", 'postgres', '1', {AutoCommit => 1}) or die "Cannot connect to database: ". DBI->errstr;
        my $res_log = $dbh->selectall_arrayref(
            "SELECT * FROM log WHERE LOWER(address) ILIKE ? ORDER BY int_id DESC, created DESC LIMIT 101",
            { Slice => {} },
            $who
        ) or die $dbh->errstr;

        my $res_messages = $dbh->selectall_arrayref(
            "SELECT * FROM message WHERE LOWER(str) ILIKE ? ORDER BY int_id DESC, created DESC LIMIT 101",
            { Slice => {} },
            $who
        ) or die $dbh->errstr;

        my @res = (@$res_messages, @$res_log);
        @res = sort {
            ($a->{int_id} cmp $b->{int_id}) ||
            ($a->{created} cmp $b->{created})
        } @res;

        # No templates. Simplifying script
        print "Content-type: text/html\n\n";
        print "<html>
        <head><title>Found receivers</title></head>
        <body>
        <h1>Found</h1>";

        if (scalar @res > 100) {
            print "<b>Too many records (more than 100)</b>";
        }

        print "<ul>
        ";
        for (my $i=0; $i<100; $i++) {
            next if !$res[$i];
            print '<li><i>' . $res[$i]->{'created'} . '</i> ';
            print '<i>' . $res[$i]->{'str'} . '</i></li>';
        }

        print "</ul>";

        print "
        </body>
        </html>";
    }

    sub root {
        print "Content-type: text/html\n\n";
        print "<html>
            <head><title>Welcome</title></head>
            <body>
            <form action='/submit' method='GET'>
                <p>Input text: <input name='search' type='text'><input type='submit' name='submit'></p>
            </form>
            </body>
            </html>";
    }


}

# start the server on port 8080
my $pid = MyServer->new(8080)->background();
print "Use 'kill $pid' to stop server.\n";
