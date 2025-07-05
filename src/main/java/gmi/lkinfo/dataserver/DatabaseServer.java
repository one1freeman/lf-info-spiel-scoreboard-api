package gmi.lkinfo.dataserver;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.net.ServerSocket;

public class DatabaseServer{
    private static String DB_PASSWORD = "nohax123";
    private static String HOST = "jdbc:mariadb://172.17.0.2:3306/game";
    private static String DB_USERNAME = "client";

    private Socket socket;
    private ServerSocket server;
    private DataInputStream in;
    private DataOutputStream out;
    private static int PORT = 33060;

    private ArrayList<String> usernames;
    private ArrayList<Integer> highscores;
    

    public DatabaseServer() {
        usernames = new ArrayList<>();
        highscores = new ArrayList<>();
    }
    
    /**
     * Loop für das Bearbeiten von allen Netzanfragen
     */
    public void run() {
        System.out.println("Hello World!");
        try {
            server = new ServerSocket(PORT);
            System.out.println("Server gestartet");
            
        } catch (Exception e) {
            System.out.println(e);
        }

        
        boolean error = false;
        while(true) {
            error = false;
            try {
                System.out.println("Auf Client warten");
                socket = server.accept();
                System.out.println("Client verbunden: " + socket.getInetAddress().getHostAddress());

                in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
                out = new DataOutputStream(socket.getOutputStream());
                String line = "";
                line = in.readUTF();
                System.out.println(line);
                
                line = in.readUTF();
                System.out.println(line);
                if(line.equals("$getdata")) {
                    if (updateData()) {
                        out.writeUTF("1000");
                        sendData();
                    } else {
                        out.writeUTF("1002");
                    }
                } else if (line.equals("$sethighscore")){           // 
                    String username = in.readUTF();
                    System.out.println(username);
                    int newHighscore = Integer.parseInt(in.readUTF());
                    System.out.println(newHighscore);
                    int highscoreError = setHighscore(username, newHighscore);
                    error = highscoreError == 2;
                    Thread.sleep(200);
                    out.writeUTF(String.valueOf(1000 + highscoreError));
                } else {
                    error = true;
                }

                if (!error) {
                    System.out.println("Client Served: " + socket.getInetAddress().getHostAddress());
                } else {
                    System.out.println("Some Error occured with: " + socket.getInetAddress().getHostAddress());
                }

                socket.close();
                in.close();
                out.close();
                
            } catch (Exception e) {
                e.printStackTrace();
                error = true;
            }

            if (error) {
                System.out.print("Error! Security cooldown: ");
                for (int i = 3; i > 0; i--) {
                    System.out.print(i + "... ");
                    try {Thread.sleep(1000);} catch (Exception e) {
                        System.out.println(e);
                    }
                }
                System.out.println("");
            }
        }
    }

    /**
     * Holt alle Daten vom DatenbankServer ein
     * @return Erfolgreich?
     */
    private boolean updateData() {
        try (Connection conn = DriverManager.getConnection(HOST, DB_USERNAME, DB_PASSWORD)) {
            
            PreparedStatement statement;
            ResultSet rs;

            
            // get alle highscores und usernames;
            // Befehl wird vorbereitet und ausgeführt
            statement = conn.prepareStatement("select username, highscore from users order by highscore desc;");
            rs = statement.executeQuery();

            // aus allen verfügbaren zeilen werden die daten gespeichert
            usernames.clear();
            highscores.clear();
            while (rs.next()) {
                usernames.add(rs.getString("username"));
                highscores.add(rs.getInt("highscore"));
            }

            conn.close();
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }

        return true;
    }

    private void sendData() throws IOException {
        for (int i = 0; i < usernames.size(); i++) {
            out.writeUTF(usernames.get(i));
        }
        out.writeUTF("$endusernames");

        for (int i = 0; i < highscores.size(); i++) {
           out.writeUTF(String.valueOf(highscores.get(i)));
        }
        out.writeUTF("$endhighscores");
    }


    /**
     * Setzt für einen Benutzer einen neuen Highscore in der Datenbank, wenn der neue Highscore größer ist, als der alte Wert
     * @param username
     * @param newHighscore
     * @return Fehlercode (0 - ok
     *                     2 - Fehler mit Datenbankverbindung
     *                     4 - Nickname ist bererits höherer Highscore zugeordnet)
     */
    private int setHighscore (String username, int newHighscore) {
        int error = 0;
        try (Connection conn = DriverManager.getConnection(HOST, DB_USERNAME, DB_PASSWORD)) {
            
            PreparedStatement statement;
            ResultSet rs;

            
            // Befehl wird vorbereitet und ausgeführt
            statement = conn.prepareStatement("select highscore from users where username = '" + username + "';");
            rs = statement.executeQuery();

            // wenn es bereits einen Eintrag gibt muss überprüft werden, ob er kleiner als der neue wert ist
            if (rs.next() && rs.getInt("highscore") >= newHighscore) {
                error = 4;
            } else {
                statement = conn.prepareStatement("replace into users values ('" + username + "', " + newHighscore +");");
                rs = statement.executeQuery();
            }

            conn.close();
        } catch (Exception e) {
            e.printStackTrace();
            error = 2;
        }

        return error;
    }
}