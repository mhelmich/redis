start_server {tags {"sl"}} {

    proc create_sl {key items} {
        r del $key
        foreach {score entry} $items {
            r sladd $key $score $entry
        }
    }

    # tests basic functionality of sladd and slcard
    test "skiplist SLADD" {
        r del sl
        r sladd sl score1 value1 score2 value2 score3 value3 score4 value4
        assert_equal {4} [ r slcard sl ]
        
        # try to add several scores again
        # redis should return 0 and not add these scores again
        assert_equal {0} [ r sladd sl score1 value1 score3 value3 ]
        assert_equal {4} [ r slcard sl ]
        r del sl
    }
    
    # tests sladd and slsearch in a small scale
    test "skiplist SLADD and SLSEARCH" {
        r del sl
        r sladd sl score1 value1
        r sladd sl score2 value2
        r sladd sl score3 value3
        
        assert_equal {score1 value1} [ r slsearch sl score1 ]
        assert_equal {score2 value2} [ r slsearch sl score2 ]
        assert_equal {score3 value3} [ r slsearch sl score3 ]
        assert_equal {3} [ r slcard sl ]
        
        create_sl sl { score1 value1 score2 value2 score3 value3 }
        r sladd sl score2 value22
        r sladd sl score2 value222
        assert_equal {5} [ r slcard sl ]
        assert_equal {score1 value1 score2 value2 score2 value22 score2 value222 score3 value3} [ r slall sl ]
        
        r del sl
    }

    test "skiplist SLREM" {
        create_sl sl { score1 value1 score2 value2 score3 value3 }
        assert_equal {3} [r slcard sl ]
        # remove out of the middle
        r slrem sl score2
        assert_equal {score1 value1 score3 value3} [ r slall sl ]
        # remove all remaining items
        r slrem sl score1
        r slrem sl score3
        assert_equal {0} [ r slcard sl ]
    
        create_sl sl { score1 value1 score2 value2 score2 value22 score2 value222 score3 value3 }
        assert_equal {5} [ r slcard sl ]
        # remove a series of elements
        r slrem sl score2
        assert_equal {score1 value1 score3 value3} [ r slall sl ]
        
        r del sl
    }

    # more comprehensively slsearch
    # inserts 5k elements into the skiplist
    # and searches for them again
    test "skiplist SLSEARCH" {
        set elements 5000
        set aux {}
        set err 0
        
        # insert all elements into the skiplist
        set scorebase "score_"
        r del searchtest
        for {set i 0} {$i < $elements} {incr i} {
            set score [concat $scorebase$i]
            r sladd searchtest $score $i
            lappend aux $score
        }
        
        # assert on the size of the skiplist afterwards
        assert_equal $elements [ r slcard searchtest ]
    
        # ask for these elements        
        for {set i 0} {$i < $elements} {incr i} {
            set rand [randomInt $elements]
            set randelement [lindex $aux $rand]
            set ele [concat $randelement $rand]
            set fromredis [r slsearch searchtest $randelement]
            if {$ele != $fromredis} {
               incr err
               puts "found assertion failure!"
               puts "expected: $ele"
               puts "got: $fromredis"
            }
        }
        
        # assert of the absence of errors
        assert_equal 0 $err
        r del searchtest
    }
    
    # this test focuses on inserting the same score multiple times
    # and making sure that always the same number of results are retrieved
    # since a skip list is a probabilistic data structure by nature,
    # this test is prone to be a flapper :(
    #
    # TODO: make this work :)
    #
    test "skiplist SLSEARCH dups" {
        set numRuns 10    
        set elements 5
        
        for {set j 0} {$j < $numRuns} {incr j} {
            r del sl
            for {set i 0} {$i < $elements} {incr i} {
                r sladd sl myscore $i
            }
            
            assert_equal {5} [ r slcard sl ]
            #assert_equal {myscore 0 myscore 1 myscore 2 myscore 3 myscore 4 narf} [ r slsearch sl myscore ]
        }
    }
    
    test "skiplist SLRANGE - no dups" {
        create_sl sl { score1 value1 score2 value2 score3 value3 score4 value4 score5 value5 score6 value6 }
        assert_equal {6} [r slcard sl ]
        
        # queries inside of the value range
        assert_equal {score2 value2 score3 value3 score4 value4} [ r slrange sl \[score2 \[score4 ]
        assert_equal {score2 value2 score3 value3} [ r slrange sl \(score1 \[score3 ]
        assert_equal {score2 value2} [ r slrange sl \(score1 \(score3 ]
        assert_equal {score3 value3} [ r slrange sl \(score2 \(score4 ]        
        assert_equal {score1 value1 score2 value2 score3 value3 score4 value4 score5 value5} [ r slrange sl score1 score5 ]
        assert_equal {score3 value3 score4 value4} [ r slrange sl score3 \(score5 ]
        assert_equal {score1 value1} [ r slrange sl r1 score1 ]
        assert_equal {score6 value6} [ r slrange sl score6 t1 ]
        assert_equal {score1 value1} [ r slrange sl \(r1 score1 ]
        
        # queries with +/- inf
        #assert_equal {score1 value1 score2 value2 score3 value3 score4 value4 score5 value5 score6 value6} [ r slrange sl - + ]
        #assert_error "*min*max*not*valid" [ r slrange sl + - ]
        
        # queries with non-existing scores
        assert_equal {score2 value2 score3 value3} [r slrange sl score11 score3]
        assert_equal {score1 value1 score2 value2 score3 value3 score4 value4 score5 value5 score6 value6} [ r slrange sl r1 t1 ]
        
        # queries out of value range
        assert_equal {} [r slrange sl t1 t2]
        assert_equal {} [r slrange sl r1 r2]
        assert_equal {} [ r slrange sl \(score6 t1 ]
        assert_equal {} [ r slrange sl r1 \(score1 ]
        
        r del sl
    }
    
    # test with dup scores
    test "skiplist SLRANGE - with dups" {
        create_sl sl { score1 value1 score2 value2 score3 value3 score4 value4 score5 value5 score6 value6 }
        r sladd sl score1 value11 score3 value33 score5 value55
        assert_equal {9} [r slcard sl ]
        assert_equal {score1 value1 score1 value11 score2 value2} [ r slrange sl score1 score2 ]
        assert_equal {score2 value2 score3 value3 score3 value33} [ r slrange sl score2 score3 ]
        assert_equal {score2 value2 score3 value3 score3 value33 score4 value4} [ r slrange sl score2 score4 ]
        
        create_sl sl { score1 value1 score1 value2 score1 value3 score1 value4 score1 value5 score2 value6 }
        assert_equal {6} [r slcard sl ]
        assert_equal {score1 value1 score1 value2 score1 value3 score1 value4 score1 value5 score2 value6} [ r slrange sl score1 score2 ]
        
        r del sl
    }
    
    test "skiplist after DEBUG RELOAD" {
        set elements 5000
        set aux {}
        set err 0
        
        # insert all elements into the skiplist
        set scorebase "score_"
        r del sl
        for {set i 0} {$i < $elements} {incr i} {
            set score [concat $scorebase$i]
            r sladd sl $score $i
            lappend aux $score
        }
        
        # assert on the size of the skiplist afterwards
        assert_equal $elements [ r slcard sl ]
        # force reload the contents of the cache
        r debug reload
        # assert on the size of the skiplist after reloading
        assert_equal $elements [ r slcard sl ]

    
        # ask for these elements        
        for {set i 0} {$i < $elements} {incr i} {
            set rand [randomInt $elements]
            set randelement [lindex $aux $rand]
            set ele [concat $randelement $rand]
            set fromredis [r slsearch sl $randelement]
            if {$ele != $fromredis} {
               incr err
               puts "found assertion failure!"
               puts "expected: $ele"
               puts "got: $fromredis"
            }
        }
        
        # assert of the absence of errors
        assert_equal 0 $err
        r del sl
    }
}
