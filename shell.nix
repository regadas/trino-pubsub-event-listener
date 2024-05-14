{ pkgs ? import <nixpkgs-unstable> { } }:

pkgs.mkShell {
  buildInputs = [
    (pkgs.gradle.override { java = pkgs.jdk22; })
    pkgs.jdk22
  ];
}
