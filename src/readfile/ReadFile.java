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
import java.sql.* ;



/**
 *
 * SQL used to setup tables in command line -
 *
 * CREATE TABLE artist ( ‘artist_id’ SERIAL PRIMARY KEY,
 * ‘name’ VARCHAR(200)
 * );
 *
 * CREATE TABLE album ( ‘album_id’ SERIAL PRIMARY KEY,
 * ‘name’ VARCHAR(200),
 * ‘artist_id’ INT  references artist(artist_id)
 * );
 *
 * CREATE TABLE song ( ‘song_id’ SERIAL PRIMARY KEY,
 * ‘name’ VARCHAR(200),
 * ‘artist_id’ INT references artist(artist_id),
 * ‘album_id’ INT  references album(album_id)
 * );
 *
 * CREATE TABLE tag ( ‘tag_id’ SERIAL PRIMARY KEY,
 * ‘name’ VARCHAR(200)
 * );
 *
 * CREATE TABLE song_tag ( ‘song_tag_id’ SERIAL PRIMARY KEY,
 * ‘artist_id’ INT references song(song_id),
 * ‘album_id’ INT references tag(tag_id)
 * );
 *
 * -------------------------------------------------------------------------
 *
 * @author irum
 */
public class ReadFile {

    public static Connection connect(){
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException ex) {
            System.out.println("Driver not found");
        }
        System.out.println("PostgreSQL driver registered.");
        Connection conn = null;
        try {
            conn = DriverManager.getConnection("jdbc:postgresql://mod-fund-databases.cs.bham.ac.uk/ltd613", "ltd613", "6j5n5ptpla");
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        if (conn != null) {
            System.out.println("Database accessed!");
        } else {
            System.out.println("Failed to make connection");
        }

        return conn;
    }

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        // TODO code application logic here

        Connection conn = connect();

        String line;
        String splitter=",";
        List<String[]> list = new ArrayList<>();

        try( BufferedReader br=new BufferedReader(new FileReader("artists-songs-albums-tags.csv")) ) {

            while((line=br.readLine())!=null){

                list.add(line.split(splitter));  // to separate each word afer looking " , " in the file and make it as token
            }

            for (String [] a : list){
                for(String s : a ){
                    System.out.print(s + " ") ;
                }

                System.out.println();
            }
        }


        catch (FileNotFoundException e){
            System.out.println("File not found");
        }
        catch (IOException ex) {
            System.out.println(ex.getMessage()+"Error reading file");
        }
    }

}
