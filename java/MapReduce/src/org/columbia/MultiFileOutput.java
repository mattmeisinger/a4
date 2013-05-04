package org.columbia;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;


public class MultiFileOutput extends MultipleTextOutputFormat<Text, Text> {
    protected String generateFileNameForKeyValue(Text key, Text value, String name) {
        return key.toString() + ".txt";
    }
}