/* une PROPOSITION de squelette, incomplète et adaptable... */

package hdfs;
import formats.Format;
import formats.KV;
import formats.KVFormat;
import formats.LineFormat;
import config.Config;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;

public class HdfsClient {
	
    private static void usage() {
        System.out.println("Usage: java HdfsClient read <file>");
        System.out.println("Usage: java HdfsClient write <line|kv> <file>");
        System.out.println("Usage: java HdfsClient delete <file>");
    }
	
    public static void HdfsDelete(String hdfsFname) {
    	int nombre_fragments = Config.tab_serveurs.length;
    	Socket socket;
	    for (int i = 0; i < nombre_fragments; i++) 
	    {
			try {
				socket = new Socket(Config.tab_serveurs[i],Config.tab_ports[i]);
				ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
				oos.writeObject("CMD_DELETE" + " " +hdfsFname);
				oos.close();
	            socket.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
	    }
    }
	
    public static void HdfsWrite(Format.Type fmt, String localFSSourceFname, 
     int repFactor)
	{
	    // Récupérer le fichier, calculer le nombre de fragments.
	    // PATH, tab_serveurs sont des variables définies dans config/Parametres.java 
	    File f = new File (Config.PATH + localFSSourceFname);
	    long taille_fichier = f.length();
	    int nombre_fragments = Config.tab_serveurs.length;
	    
	    int taille_fragment = (int) Math.ceil(taille_fichier / nombre_fragments);
	
	    if (fmt == Format.Type.LINE)
	    {
            LineFormat fichier = new LineFormat(localFSSourceFname);
            fichier.open(Format.OpenMode.R);
            Socket socket;
            ObjectOutputStream oos;
	        for (int i = 0 ; i < nombre_fragments ; i++)
	        {
		    // Construire le fragment i
		    int c = 0;
	            String texte_fragment = "";
		    KV kv = new KV(" ", " "); // Pour lire le fichier
		    
		    while (kv != null && c < taille_fragment)
		    {
				// Lire une ligne
				kv = fichier.read();
				texte_fragment += kv.v;
				texte_fragment += "\n";
	
				// Augmenter le compteur
				c += kv.v.length();
			    }
	
			    // Envoyer le fragment i sur la machine i
				try {
					socket = new Socket (Config.tab_serveurs[i], Config.tab_ports[i]);
					oos = new ObjectOutputStream(socket.getOutputStream());
					// Commande : CMD_WRITE nom_fichier&LINE&texte_du_fragment
					oos.writeObject("CMD_WRITE" + " " + localFSSourceFname + "&LINE&" + texte_fragment);
					oos.close();
					socket.close();

				} catch (IOException e) {
					e.printStackTrace();
				}    
	        }    
		fichier.close();
        }
	    
	    else // fmt == Type.KV
	    {
	        KVFormat fichier = new KVFormat(localFSSourceFname);
	        fichier.open(Format.OpenMode.R);
		
			for (int i = 0 ; i < nombre_fragments ; i++)
		        {
			    // Construire le fragment i
			    int c = 0;
		            String texte_fragment = "";
			    KV kv = new KV(" ", " "); // Pour lire le fichier
			    while (kv != null && c < taille_fragment)
			    {
				// Lire une ligne
				kv = fichier.read();
				texte_fragment += kv.v;
				texte_fragment += "\n";
	
				// Augmenter le compteur
				c += kv.v.length();
			    }
	
			    // Envoyer le fragment i sur la machine i
			    Socket socket;
			    ObjectOutputStream oos;
				try {
					socket = new Socket (Config.tab_serveurs[i], Config.tab_ports[i]);
					oos = new ObjectOutputStream(socket.getOutputStream());
					 // Commande : CMD_WRITE nom_fichier&KV&texte_du_fragment
				    oos.writeObject("CMD_WRITE" + " " + localFSSourceFname + "&KV&" + texte_fragment);
				    oos.close();
		            socket.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			fichier.close();
	    }
	}

    public static void HdfsRead(String hdfsFname, String localFSDestFname) throws UnknownHostException, IOException, ClassNotFoundException {
    	int n = Config.tab_serveurs.length;
    	Socket[] sockets = new Socket[n]; 
        /*Boucle initialisant chaque socket et réalisant une démande de connexion*/
        for (int i = 0; i<n; i++){
            sockets[i] = new Socket(Config.tab_serveurs[i],Config.tab_ports[i]);
            System.out.println("Connecté à la machine" + Config.tab_serveurs[i] + " sur le port " + Config.tab_ports[i]);
        }
        /*Déclaraiton des OutputStream qui serviront pour envoyer la commande aux nodes*/ 
        ObjectOutputStream[] oOutputs = new ObjectOutputStream[n];
        /*Déclaraiton des InputStream qui serviront pour envoyer la commande aux nodes*/
        ObjectInputStream[] oInputs = new ObjectInputStream[n];
        /*Definition du message qui va être envoyé par le socket*/
        String commande = "CMD_READ " + hdfsFname; 
        /*Ouverture du fichier où on va écrire*/
        Format fichier = (Format) new KVFormat(Config.PATH+localFSDestFname);
        fichier.open(KVFormat.OpenMode.W);
        /*Envoi des commandes sur les sockets et initialisation*/
        for (int i= 0;i<n;i++){
            oOutputs[i] = new ObjectOutputStream(sockets[i].getOutputStream());
            oInputs[i] = new ObjectInputStream(sockets[i].getInputStream());
            /*Écriture de la commande dans le outputStream*/ 
            oOutputs[i].writeObject(commande);
            /*Réception du premier kv qui nous indique si le fichier est présent ds le serveur*/
            String KVs = (String) oInputs[i].readObject();
            Boolean kvNull = KVs == null;
            /*Initialisation de la version classe du KV*/ 
            KV kv = null;
            String[] k_et_s;
			if (kvNull) {
				/*Si le premier KV est nul alors on sait que le serveur n'a pu envoyer aucun KV donc il a pas le fragment*/ 
                System.out.println("Aucun fragment du fichier " + hdfsFname + " trouvé dans le serveur " + i);	
			} else {
				k_et_s = KVs.split("<->");
				kv = new KV(k_et_s[0], k_et_s[1]);
			}
			while (!kvNull) {
				/**On écrit le kv qui n'est pas nul */
                fichier.write(kv);
                /**On reçoit le kv suivant */
				KVs = (String) oInputs[i].readObject();
                if (KVs == null){
                    /**C'est la fin de la lecture des kvs pour le serveur i */
                    kvNull = true;
                }
                else {
                    /**On genere l'objet kv à partir du string kv */
                    k_et_s = KVs.split("<->");
                    kv = new KV(k_et_s[0], k_et_s[1]);
                }
                    
			}	
        } 
        
        /*Fermeture de tous les sockets et Streams utilisés ainsi que le fichier sur lequel on écrit*/  
        fichier.close();
        for (int i = 0;i<n;i++){
            sockets[i].close();
	    oOutputs[i].close();
	    oInputs[i].close();
        }

    }

	
    public static void main(String[] args) {
        // java HdfsClient <read|write> <line|kv> <file>
	
        try {
	    /*Géneration du client pour ouvrir les sockets*/
            if (args.length<2) {usage(); return;}
            switch (args[0]) {
              case "read": HdfsRead(args[1],Config.PATH + args[1]); break;
              case "delete": HdfsDelete(args[1]); break;
              case "write": 
                Format.Type fmt;
                if (args.length<3) {usage(); return;}
                if (args[1].equals("line")) fmt = Format.Type.LINE;
                else if(args[1].equals("kv")) fmt = Format.Type.KV;
                else {usage(); return;}
                HdfsWrite(fmt,args[2],1);break;
              default : usage(); 
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

}
