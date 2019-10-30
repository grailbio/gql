#!/usr/bin/perl

@out = ();
while (<>) {
    chomp;
    s/\\/\\\\/g;
    s/\"/\\"/g;
    push(@out, $_);
}

open(OUT, ">lib.go") || die "lib.go: $!";
print OUT "// Generated from lib/*.gql. DO NOT EDIT.\n";
print OUT "package lib\n";
print OUT "var Script = ";
for ($i = 0; $i < @out; $i++) {
    print OUT "\t\"$out[$i]\\n\"";
    if ($i < @out - 1) {
        print OUT " +\n";
    } else {
        print OUT "\n";
    }
}
