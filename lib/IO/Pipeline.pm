package IO::Pipeline;

use strict;
use warnings FATAL => 'all';
use Scalar::Util qw(blessed);
use IO::Handle;
use Exporter ();

our @ISA = qw(Exporter);

our @EXPORT = qw(pmap pgrep psink);

sub import {
  warnings->unimport('void');
  shift->export_to_level(1, @_);
}

sub pmap (&) { IO::Pipeline->from_code_map($_[0]) }
sub pgrep (&) { IO::Pipeline->from_code_grep($_[0]) }
sub psink (&) { IO::Pipeline->from_code_sink($_[0]) }

use overload
  '|' => '_pipe_operator',
  fallback => 1;

sub IO::Pipeline::CodeSink::print {
  my $code = (shift)->{code};
  foreach my $line (@_) {
    local $_ = $line;
    $code->($line);
  }
}

sub from_code_map {
  bless({ map => [ $_[1] ] }, $_[0]);
}

sub from_code_grep {
  my ($class, $grep) = @_;
  $class->from_code_map(sub { $grep->($_) ? ($_) : () });
}

sub from_code_sink {
  bless({ code => $_[1] }, 'IO::Pipeline::CodeSink');
}

sub _pipe_operator {
  my ($self, $other, $reversed) = @_;
  if (blessed($other) && $other->isa('IO::Pipeline')) {
    my ($left, $right) = $reversed ? ($other, $self) : ($self, $other);
    my %new = (map => [ @{$left->{map}}, @{$right->{map}} ]);
    die "Right hand side has a source, makes no sense"
      if $right->{source};
    $new{source} = $left->{source} if $left->{source};
    die "Left hand side has a sink, makes no sense"
      if $left->{sink};
    $new{sink} = $right->{sink} if $right->{sink};
    return bless(\%new, ref($self));
  } else {
    my ($is, $isnt) = $reversed ? qw(source sink) : qw(sink source);
    if (my $fail = $self->{$is}) {
      die "Tried to add ${is} ${other} but we already had ${fail}";
    }
    my $new = bless({ $is => $other, %$self }, ref($self));
    if ($new->{$isnt}) {
      $new->run;
      return;
    } else {
      return $new;
    }
  }
}

sub run {
  my ($self) = @_;
  my $source = $self->{source};
  my $sink = $self->{sink};
  LINE: while (defined(my $line = $source->getline)) {
    my @lines = ($line);
    foreach my $map (@{$self->{map}}) {
      @lines = map $map->($_), @lines;
      next LINE unless @lines;
    }
    $sink->print(@lines);
  }
}

1;
