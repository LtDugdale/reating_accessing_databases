/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package readfile;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


/**
 *
 * @author irum
 */
public class ReadFile {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        // TODO code application logic here
        String line;
        String splitter=",";
        String[] token;

        List<String> list=new ArrayList<String>();
        try {
          /*
           to read data from file
           */
            BufferedReader br=new BufferedReader(new FileReader("artists-songs-albums-tags.csv"));

            while((line=br.readLine())!=null){
                token=line.split(splitter);  //to separate each word afer looking " , " in the file and make it as token
                for (int i=0;i<token.length;i++){
                    list.add(token[i]);
                }

            }
            // convert the list into array
            String[] arrString=list.toArray(new String[list.size()]);
            for (int  i=0; i<list.size();i++){
                System.out.println(arrString[i]);
            }

            br.close();
        }


        catch (FileNotFoundException e){
            System.out.println("File not found");
        }
        catch (IOException ex) {
            System.out.println(ex.getMessage()+"Error reading file");
        }
    }

}
