(function() {
    jsTest.log('Testing running import with headerline');

    var toolTest = new ToolTest('import_writes');
    var db1 = toolTest.startDB('foo');

    var db = db1.getDB().getSiblingDB("droptest")

    // Verify that --drop works.
    // put a test doc in the collection, run import with --drop,
    // make sure that the inserted doc is gone and only the imported
    // docs are left.
    db.c.insert({x:1})
    assert.eq(db.c.count(), 1, "collection count should be 1 at setup")
    var ret = toolTest.runTool("import", "--file",
      "jstests/import/testdata/csv_header.csv",
      "--type=csv",
      "--db", db.getName(),
      "--collection", db.c.getName(),
      "--headerline",
      "--drop")

    // test csv file contains 3 docs and collection should have been dropped, so the doc we inserted
    // should be gone and only the docs from the test file should be in the collection.
    assert.eq(ret, 0)
    assert.eq(db.c.count(), 3)
    assert.eq(db.c.count({x:1}),0)

    // --drop on a non-existent collection should not cause error
    db.c.drop()
    var ret = toolTest.runTool("import", "--file",
      "jstests/import/testdata/csv_header.csv",
      "--type=csv",
      "--db", db.getName(),
      "--collection", db.c.getName(),
      "--headerline",
      "--drop")
    assert.eq(ret, 0)
    assert.eq(db.c.count(), 3)

    toolTest.stop();
}());
