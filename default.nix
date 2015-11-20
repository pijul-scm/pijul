with (import <nixpkgs> {});

rustUnstable.buildRustPackage rec {
  name = "rust-pijul-${version}";
  version = "0.0";
  src = ./.;

  buildInputs = [ lmdb openssl libssh2 zlib pkgconfig ];
  
  depsSha256 = "0g8hh29dxsq81h2zjri67jzmbg660ja3hif776vn341zknmq1n17";
  
}
