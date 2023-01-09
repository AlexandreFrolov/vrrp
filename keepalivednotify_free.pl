#!/usr/bin/perl
use strict;
use warnings;
use File::Slurp;
use File::Copy;
use Sys::Hostname;
use Data::Dumper;

my $conf=
{
  'mysql_cmd' => '/usr/bin/mysql -s -uroot -pwJzBruD6WOzV9VCx -e ',
  'mysql_config_file' => '/etc/mysql/mariadb.conf.d/50-server.cnf',
  'mysql_master_config_file' => '/home/frolov/vrrp/mysql_config/50-server.cnf_master',
  'mysql_slave_config_file' => '/home/frolov/vrrp/mysql_config/50-server.cnf_slave',
  'mysql_slave_no_replication_config_file' => '/home/frolov/vrrp/mysql_config/50-server.cnf_slave_no_replication',
};

# ========================================================================================================
# relay_log_done
# Проверяем, все ли команды из raley_log выполнены на SLAVE 
# ========================================================================================================
sub relay_log_done
{
  my ($conf) = @_;
  my $cmd = $conf->{mysql_cmd}."\'SHOW PROCESSLIST'".'|grep "Slave has read all relay log; waiting for more updates"';
  my @rqout;
  for(my $i=0; $i <= 5; $i++)
  {
    @rqout = ();
    @rqout = split /\n/, `$cmd`;
    if(defined $rqout[0]) { return 1; }
    sleep(3);
  }
  return 0;
}


# ========================================================================================================
# stop_replica
# Останавливаем репликацию на SLAVE
# ========================================================================================================
sub stop_replica
{
  my ($conf) = @_;
  my $rc = {};

  my $cmd = $conf->{mysql_cmd}."\'stop slave IO_THREAD'";
  my @rqout = ();
  @rqout = split /\n/, `$cmd`;

  if(relay_log_done($conf))
  {
    $cmd = $conf->{mysql_cmd}."\'STOP SLAVE'";
    @rqout = ();
    @rqout = split /\n/, `$cmd`;

    $cmd = $conf->{mysql_cmd}."\'RESET MASTER'";
    @rqout = ();
    @rqout = split /\n/, `$cmd`;

    if(!copy($conf->{ 'mysql_slave_no_replication_config_file'}, $conf->{'mysql_config_file'}))
    {
      $rc->{ 'err_message' } = "MySQL config file copy failed: $!";
      $rc->{ 'rc' } = 'err';
      return($rc);
    }
    $rc->{ 'rc' } = 'ok';
  }
  else
  {
    $rc->{ 'rc' } = 'err';
    $rc->{ 'err_message' } = "Slave can not read all relay log";
  }
  return($rc);
}


# ===============================================================================================
# START
# perl keepalivednotify.pl INSTANCE my_site MASTER
# ===============================================================================================

my $datestring = localtime();
my $fh;

# Log file for keepalive daemon
my $log_file = '/home/frolov/node_keepalive_log.txt';
open($fh, '>>:encoding(UTF-8)', $log_file) or die "Can't open file '$log_file'";

# Node state file
my $node_keepalive_state_file = '/home/frolov/node_keepalive_state.txt';

my $instance = $ARGV[0];
my $group = $ARGV[1];
my $state = $ARGV[2];
my $host = hostname; # m01host.itmatrix.ru - MASTER, s01host.itmatrix.ru - BACKUP

my $state_str = $instance.'_'.$group.'_'.$state.','.$host.','.$datestring."\n";
write_file($node_keepalive_state_file, $state_str);

print $fh "$datestring: $host ($instance $group) -> $state\n";

if($host eq 's01host.itmatrix.ru' and $state eq 'MASTER')   # BACKUP go to MASTER
{
  my $rc = stop_replica($conf);
  print $fh "$datestring: $host ($instance $group) -> Stop MySQL Replication\n";
}
elsif($host eq 's01host.itmatrix.ru' and $state eq 'BACKUP')   # BACKUP return from MASTER to BACKUP
{
  print $fh "$datestring: $host ($instance $group) -> BACKUP node return from MASTER to BACKUP\n";
}
elsif($host eq 's01host.itmatrix.ru' and $state eq 'STOP')   # BACKUP return from MASTER to BACKUP
{
  print $fh "$datestring: $host ($instance $group) -> BACKUP node goto STOP\n";
}
elsif($host eq 'm01host.itmatrix.ru') # MASTER state
{
  print $fh "$datestring: $host ($instance $group) -> MASTER node goto $state\n";
}
else
{
  print $fh "$datestring: $host ($instance $group) -> Invalid host $host or invalid state $state\n";
}
close $fh;

