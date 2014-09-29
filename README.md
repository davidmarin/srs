Common code for spendright-scrapers
===================================

This is a small library containing common code for the various spendright
scrapers.

For now, the best approach is to include this as a submodule, and link
to the srs package::

    # From the top level of your git repo:
    mkdir submodules
    git submodule add git@github.com:spendright-scrapers/srs.git submodules/srs
    ln -s submodules/srs/srs
