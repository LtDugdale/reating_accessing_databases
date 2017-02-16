package readfile;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.sql.*;

/**
 *
 * @author Laurie Dugdale
 */
public class ReadFile {

    private Connection conn; // The established connection to the database
    private String path; // path to the CSV file

    /**
     * Constructor for ReadFile class
     *
     * @param url The url of the database.
     * @param username The database username.
     * @param pass Your database password.
     * @param path The path of the file.
     */
    public ReadFile(String url, String username, String pass, String path){

        this.path = path;
        Connection conn = null;

        try {

            Class.forName("org.postgresql.Driver");
            conn = DriverManager.getConnection(url, username, pass);
        } catch (ClassNotFoundException ex) {

            System.out.println("Driver not found");
        } catch (SQLException ex) {

            ex.printStackTrace();
        }

        this.conn = conn;
    }


    /*
     *  Getters and Setters
     */
    /**
     * getter for conn field variable
     *
     * @return conn field variable
     */
    public Connection getConn(){
        return conn;
    }

    /**
     * Getter for path field variable
     *
     * @return the path to the csv file
     */
    public String getPath(){
        return this.path;
    }

    /*
     * Main instance methods
     */
    /**
     * BURN EVERYTHING - this method drops all tables created in this class from the database
     *
     * @throws SQLException
     */
    public void dropTables() throws SQLException {

        PreparedStatement drop = getConn().prepareStatement(
                "DROP TABLE IF EXISTS artist CASCADE"
        );
        drop.execute();

        drop = getConn().prepareStatement(
                "DROP TABLE IF EXISTS album CASCADE"
        );
        drop.execute();

        drop = getConn().prepareStatement(
                "DROP TABLE IF EXISTS song CASCADE"
        );
        drop.execute();

        drop = getConn().prepareStatement(
                "DROP TABLE IF EXISTS tag CASCADE"
        );
        drop.execute();

        drop = getConn().prepareStatement(
                "DROP TABLE IF EXISTS song_tag CASCADE"
        );
        drop.execute();

        drop = getConn().prepareStatement(
                "DROP TABLE IF EXISTS temp_album CASCADE"
        );
        drop.execute();
        drop.close();

    }

    /**
     * Method to drop connection in main method
     *
     * @throws SQLException passing on exception to the main method
     */
    public void dropConnection() throws SQLException {
        conn.close();
    }

    /**
     * Creates the tables to hold the data from the CSV file
     *
     * @throws SQLException passing on exception to the main method
     */
    public void createTables() throws SQLException {

            // setup artist table
            PreparedStatement artist = getConn().prepareStatement(
                "CREATE TABLE IF NOT EXISTS artist (artist_id SERIAL PRIMARY KEY," +
                "name VARCHAR(200)" +
                ")"
            );

            // setup album table
            PreparedStatement album = getConn().prepareStatement(
                "CREATE TABLE IF NOT EXISTS album (album_id SERIAL PRIMARY KEY," +
                "name VARCHAR(200)," +
                "artist_id INT  references artist(artist_id)" +
                " )"
            );

            // setup song table
            PreparedStatement song = getConn().prepareStatement(
                "CREATE TABLE IF NOT EXISTS song ( song_id SERIAL PRIMARY KEY," +
                "name VARCHAR(200)," +
                "artist_id INT references artist(artist_id)," +
                "album_id INT  references album(album_id)" +
                ")"
            );

            // setup tag table
            PreparedStatement tag = getConn().prepareStatement(
                "CREATE TABLE IF NOT EXISTS tag ( tag_id SERIAL PRIMARY KEY," +
                "name VARCHAR(200)" +
                ")"
            );

            // setup songTag table
            PreparedStatement songTag = getConn().prepareStatement(
                "CREATE TABLE IF NOT EXISTS song_tag (song_tag_id SERIAL PRIMARY KEY," +
                "song_id INT references song(song_id)," +
                "tag_id INT references tag(tag_id)" +
                ");"
            );

            PreparedStatement tempAlbum = getConn().prepareStatement(
                    "CREATE TABLE IF NOT EXISTS temp_album (album_id SERIAL PRIMARY KEY," +
                    "name VARCHAR(200)," +
                    "artist_id INT  references artist(artist_id)" +
                    " )"
            );

            // execute sql for creating tables
            artist.execute();
            album.execute();
            song.execute();
            tag.execute();
            tempAlbum.execute();
            songTag.execute();

            artist.close();
            album.close();
            song.close();
            tag.close();
            tempAlbum.close();
            songTag.close();
    }

    /**
     * Method for iterating through the csv file and adding to the database
     *
     * @throws SQLException passing on exception to the main method
     */
    private void parseFile() throws SQLException {

        String albumName = "";
        String songName = "";
        String artistName = "";
        String tagName = "";
        String line;
        List<String[]> list = new ArrayList<>();

        PreparedStatement tempAlbum = getConn().prepareStatement(
                "INSERT INTO temp_album (name, artist_id) VALUES (? , (SELECT artist_id FROM artist WHERE name=? )) "
        );

        PreparedStatement addArtist = getConn().prepareStatement(
                "INSERT INTO artist (name) SELECT (?) WHERE NOT EXISTS( SELECT * FROM artist WHERE name=?)"
        );

        PreparedStatement addAlbum = getConn().prepareStatement(
                "INSERT INTO album (name, artist_id) SELECT DISTINCT name, artist_id FROM temp_album"
        );

        PreparedStatement addSong = getConn().prepareStatement(
                "INSERT INTO song (name, artist_id, album_id) VALUES ( ?," +
                "(SELECT artist_id FROM artist WHERE name=?)," +
                "(SELECT album_id FROM album JOIN artist ON artist.artist_id=album.artist_id WHERE album.name=? AND artist.name = ?))"
        );

        PreparedStatement addTag = getConn().prepareStatement(
                "INSERT INTO tag (name) SELECT (?) WHERE NOT EXISTS( SELECT * FROM tag WHERE name=?)"
        );

        PreparedStatement songTags = getConn().prepareStatement("INSERT INTO song_tag(song_id, tag_id) VALUES (" +
                "(SELECT song.song_id FROM song JOIN album ON song.album_id=album.album_id WHERE song.name=? AND album.name=?)," +
                "(SELECT tag.tag_id FROM tag WHERE tag.name = ?)" +
                ")"
        );

        try( BufferedReader br=new BufferedReader(new FileReader(getPath())) ) {

            br.readLine(); // skip first line
            while((line=br.readLine())!=null){

                // to separate each word after looking " , " in the file and add the created array to list.
                list.add(line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1));
            }

            for (String [] a : list) {


                artistName = a[1];
                albumName = a[2];

                // add variables and execute addArtist
                addArtist.setString(1, artistName);
                addArtist.setString(2, artistName);
                addArtist.execute();

                // add variables and execute tempAlbum
                tempAlbum.setString(1, albumName);
                tempAlbum.setString(2, artistName);
                tempAlbum.execute();


            }

            // execute addAlbum
            addAlbum.execute();

            for (String [] a : list) {

                songName = a[0];
                artistName = a[1];
                albumName = a[2];

                // add variables and execute addSong
                addSong.setString(1, songName);
                addSong.setString(2, artistName);
                addSong.setString(3, albumName);
                addSong.setString(4, artistName);
                addSong.execute();

                for (int i = 3; i < a.length; i++){

                    tagName = a[i];

                    // add variables and execute addTag
                    addTag.setString(1, tagName);
                    addTag.setString(2, tagName);
                    addTag.execute();

                    // add variables and execute songTags
                    songTags.setString(1, songName);
                    songTags.setString(2, albumName);
                    songTags.setString(3, tagName);
                    songTags.execute();
                }
            }

            PreparedStatement dropTempAlbum = getConn().prepareStatement(
                    "DROP TABLE temp_album CASCADE"
            );
            dropTempAlbum.execute();

            // close prepared statements
            dropTempAlbum.close();
            tempAlbum.close();
            addArtist.close();
            addAlbum.close();
            addSong.close();
            addTag.close();
            songTags.close();

        } catch (FileNotFoundException e){
            System.out.println("File not found");
        }
        catch (IOException ex) {
            System.out.println(ex.getMessage()+"Error reading file");
        }
    }

    /**
     * This method answers performs the requested SQL queries.
     *
     * @throws SQLException passing on exception to the main method
     */
    public void queries() throws SQLException {

        Statement questions = getConn().createStatement();

        // Question 1
        ResultSet q1RS = questions.executeQuery(
                "SELECT DISTINCT COUNT(album_id) FROM album"
        );
        System.out.println("1. How many albums are listed?");
        while(q1RS.next()){
            System.out.println(q1RS.getString(1));
        }

        // Question 2
        ResultSet q2RS = questions.executeQuery(
                "SELECT DISTINCT COUNT(album.name) FROM album " +
                        "JOIN song ON album.album_id=song.album_id " +
                        "JOIN song_tag ON song.song_id=song_tag.song_id " +
                        "JOIN tag ON song_tag.tag_id=tag.tag_id " +
                        "WHERE tag.name = 'classic rock*'"
        );
        System.out.println("\n2. How many albums are classic rock ones?");
        while(q2RS.next()){
            System.out.println(q2RS.getString(1));
        }

        // Question 3
        ResultSet q3RS = questions.executeQuery(
            "SELECT DISTINCT artist.name FROM artist " +
                    "JOIN song ON artist.artist_id=song.artist_id " +
                    "JOIN song_tag ON song.song_id=song_tag.song_id " +
                    "JOIN tag ON song_tag.tag_id=tag.tag_id " +
                    "WHERE tag.name='rhythmic' ORDER BY artist.name;"
        );
        System.out.println("\n3. List, in alphabetical order, all the artists who have tracks regarded as rhythmic");
        while(q3RS.next()){
            System.out.println(q3RS.getString(1));
        }

        // Question 4
        ResultSet q4RS = questions.executeQuery(
            "SELECT(" +
                    "ABS(" +
                        "(SELECT COUNT(song.name) FROM song WHERE song.name LIKE '%LOVE%')" +
                        " - " +
                        "(SELECT COUNT(tag.tag_id) FROM tag " +
                        "JOIN song_tag ON tag.tag_id=song_tag.tag_id " +
                        "JOIN song ON song.song_id=song_tag.song_id " +
                        "WHERE tag.name='love' AND song.name NOT LIKE '%LOVE%')" +
                    ")" +
            ")"
        );
        System.out.println("\n4. What is the numerical difference between songs with \"love\" in the title and those that are tagged with \"love\" that don't have it in the title?");
        while(q4RS.next()){
            System.out.println(q4RS.getString(1));
        }

        // Question 5
        ResultSet q5RS = questions.executeQuery(
                "SELECT COUNT(album.name) FROM album " +
                        "JOIN song ON song.album_id=album.album_id " +
                        "WHERE song.name LIKE '%DANCE%';"
        );
        System.out.println("\n5. How many albums have at least one song with \"dance\" in the title?");
        while(q5RS.next()){
            System.out.println(q5RS.getString(1));
        }

        // Question 6
        ResultSet q6RS = questions.executeQuery(
                "SELECT((SELECT COUNT(song_tag.song_tag_id) FROM tag JOIN song_tag ON song_tag.tag_id=tag.tag_id WHERE tag.name='playful') " +
                        "> " +
                        "(SELECT COUNT(song_tag.song_tag_id) FROM tag JOIN song_tag ON song_tag.tag_id=tag.tag_id WHERE tag.name='rhythmic')" +
                ");"
        );
        System.out.println("\n6. Are there more playful songs than rhythmic ones?");
        while(q6RS.next()){

            System.out.println(q6RS.getBoolean(1));
        }

        // close connections
        q1RS.close();
        q2RS.close();
        q3RS.close();
        q4RS.close();
        q5RS.close();
        q6RS.close();
        questions.close();



    }

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {

        ReadFile rf = new ReadFile("jdbc:postgresql://mod-fund-databases.cs.bham.ac.uk/ltd613", "ltd613", "6j5n5ptpla", "artists-songs-albums-tags.csv");
        try {

            rf.dropTables();
            rf.createTables();
            rf.parseFile();
            rf.queries();
            rf.dropConnection();

        } catch (SQLException e) {
            e.printStackTrace();
        }

    }



}
