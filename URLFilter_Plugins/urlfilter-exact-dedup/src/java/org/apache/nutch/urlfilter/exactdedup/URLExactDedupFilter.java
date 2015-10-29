package org.apache.nutch.urlfilter.exactdedup;

import org.apache.nutch.util.NutchConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.nutch.plugin.Extension;
import org.apache.nutch.plugin.PluginRepository;
import org.apache.nutch.net.URLFilter;
import org.apache.nutch.protocol.Content;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;
import org.apache.nutch.segment.SegmentReader;
import org.apache.hadoop.io.Writable;
import org.apache.commons.cli.Options;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.fs.FileSystem;

import java.lang.Boolean;
import java.lang.String;
import java.lang.System;
import java.util.*;
import java.io.*;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashSet;
import java.util.Set;

public class URLExactDedupFilter implements URLFilter {
    private Configuration configuration;
    private String args[];
    //we have to hardcode the path here since we didn't figure out 
    //how to retrieve the segments path from NutchConfiguration
    private String BASE_PATH = "./local/usc/segments/";
    SegmentReader reader = null;
    static public Set<String> md5List=new HashSet<String>();
    static public int count = 0;

    //check md5 has of content is in current set
    //return true if not in current set and add it to set
    //false otherwise
    public Boolean generateMd5Array(String content){
        String md5String=URLExactDedupFilter.getMd5(content);
        if (md5List.contains(md5String) == true) {
            return false;
        } else {
            md5List.add(md5String);
            return true;
        }
    }

    //calculate md5 hash
    public static String getMd5(String string) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            BigInteger bi = new BigInteger(1, md.digest(string.getBytes()));
            return bi.toString(16);
        } catch (NoSuchAlgorithmException ex) {
            return "";
        }
    }

    //read segment directory and filter URL
    //if the content of URL is not a duplicated one, return URL
    //null otherwise
    public String filter (String url) {
        this.configuration = NutchConfiguration.create();
        reader = new SegmentReader(this.configuration, true, true, true, false, false, false);
        Options opts = new Options();
        try {
            GenericOptionsParser parser = new GenericOptionsParser(configuration, opts, args);
            String[] remainingArgs = parser.getRemainingArgs();
            FileSystem fs = FileSystem.get(configuration);
            //String segment = remainingArgs[0];
            File segments_directories = new File(BASE_PATH);
            String[] directories = segments_directories.list(new FilenameFilter() {
                @Override
                public boolean accept(File current, String name) {
                    return new File(current, name).isDirectory();
                }
            });
            if (directories == null) {System.out.println(url); return url;}
            Path file = new Path(BASE_PATH + directories[directories.length-1]);
            Content content = readSegment(file, url);
            if (content != null) {
                if (this.generateMd5Array(content.toString()) == true) {
                    return url;
                } else {
                    System.out.println("Detected a duplicate website.");
                    System.out.println("current duplicates: " + Integer.toString(count++));
                    return null;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return url;
    }

    public void setConf(Configuration conf) {
        this.configuration = conf;
    }

    public Configuration getConf() {
        return this.configuration;
    }

    //use URL as key to retrieve the content in segments
    public Content readSegment(Path path, String url){
        Text key= new Text(url);
        Content content = null;

        ArrayList<Writable> parsedLst = null;
        Map<String,List<Writable>> results=new HashMap<String, List<Writable>>();
        try {
            reader.get(path, key, new StringWriter(), results);
            //Path temp_path = new Path("/vagrant/nutch/runtime/local/hehe/");
            //reader.dump(path, temp_path);
            parsedLst=(ArrayList<Writable>) results.get("co");
            if(parsedLst == null) {
                return content;
            }
            Iterator<Writable> parseIter=parsedLst.iterator();
            while(parseIter.hasNext()) {
                content = (Content) parseIter.next();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return content;
    }
}
