open(IN, "/dev/urandom") || die "urandom: $!";
print "hash.Hash{";
for ($i = 0; $i < 32; $i++ ) {
    read(IN, $x, 1);
    if ($i > 0) {
        print(", ")
    }
    if ($i % 8 == 0) {
        print("\n")
    }
    printf("0x%02x", ord($x));
}
printf("}\n");
