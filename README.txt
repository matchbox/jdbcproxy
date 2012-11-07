unix style jdbc proxy so you can interface with it from shell scripts

use:
JDBCProxy.class -u myuser -p mypass -d com.filemaker.Driver -c filemaker://asdfasdf.com/dbname < queries.txt

jars included:
simple-json: http://code.google.com/p/json-simple/ (Apache 2.0 license)
apache command line: http://commons.apache.org/cli/ (Apache 2.0 license)
