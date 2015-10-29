package org.apache.nutch.urlfilter.neardedup;

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

public class URLNearDedupFilter implements URLFilter {
    private Configuration configuration;
    private String args[];
    private String BASE_PATH = "./local/usc/segments/";
    SegmentReader reader = null;
    static public int count = 0;

    static public ArrayList<String> simhashList=new ArrayList<String>();
    static int wordLength = 3;
    
    //this function is for generate Simhash string
    public static String generateSimhash(String content){
        //int stepLength=wordLength;
        int [] sumHash=new int [32];
        String simString="";
        for(int i=0;i<32;i++){
            sumHash[i]=0;
        }
        for(int i=0;i+wordLength<content.length()-1;i+=wordLength){
            String word=content.substring(i, i+wordLength);
            String wordHash=Integer.toBinaryString(word.hashCode());
            while(wordHash.length()<32){
                wordHash='0'+wordHash;
            }
            //System.out.println(wordHash);
            //System.out.println(word);
            for(int j=0;j<32;j++){
                if(wordHash.charAt(j)=='1'){
                    sumHash[j]+=1;
                }else{
                    sumHash[j]-=1;
                }
            }
        }

        for(int i=0;i<32;i++){
            if(sumHash[i]<=0){
                simString+='0';
            }else{
                simString+='1';
            }
        }
        return simString;
    }

    //if sim hash String is not in the list, no duplicates, return true
    //false other wise
    public Boolean generateSimhashArray(String content,int distance){
        String simString=new URLNearDedupFilter().generateSimhash(content);
        //System.out.println(simString);

        int isDup=0;
        for(int i=0;i<simhashList.size();i++){
            if(URLNearDedupFilter.nearEquals(simString,simhashList.get(i))<=distance){
                isDup=1;
                return false;
            }
        }
        if(isDup==0){
            simhashList.add(simString);
            return true;
        } else {
            return false;
        }
    }

    private static int nearEquals(String key1, String key2) {
        // TODO Auto-generated method stub
        int distance=0;
        for(int i=0;i<key1.length();i++){
            if(key1.charAt(i)!=key2.charAt(i)){
                distance+=1;
            }
        }
        return distance;
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
                if (this.generateSimhashArray(content.toString(), 3) == true) {
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
