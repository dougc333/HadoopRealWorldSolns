package com.test;

import java.util.*;
import java.io.*;

import org.apache.hadoop.*;
import org.apache.pig.*;

import org.apache.pig.data.*;

public class IsUseragentBot extends FilterFunc {

    private Set<String> blacklist = null;
    
    private void loadBlacklist() throws IOException {
        blacklist = new HashSet<String>();
        BufferedReader in = new BufferedReader(new 
          FileReader("blacklist.txt"));
        String userAgent = null;
        while ((userAgent = in.readLine()) != null) {
            blacklist.add(userAgent);
        }
    }
    
    @Override
    public Boolean exec(Tuple tuple) throws IOException {
        if (blacklist == null) {
            loadBlacklist();
        }
        if (tuple == null || tuple.size() == 0) {
            return null;
        }
        
        String ua = (String) tuple.get(0);
        if (blacklist.contains(ua)) {
            return true;
        }
        return false;   
    }
    
}