/* une PROPOSITION de squelette, incomplète et adaptable... */

package hdfs;
import formats.Format;
import formats.KV;
import formats.KVFormat;
import formats.LineFormat;
import configuration.Config;

public class HdfsClient {
	
	
    private Socket[] sockets;
    private int n;

    /*Constructeur qui permet de définir une liste de sockets en fonction du fichier configuration défini*/ 
    public HdfsClient(){
        /*Nombre de sockets à définir*/ 
        n = Config.adresses.length;
        sockets = new Socket[n]; 
        /*Boucle initialisant chaque socket et réalisant une démande de connexion*/
        for (int i = 0; i<n; i++){
            sockets[i] = new Socket(Config.adresses[i],Config.ports[i]);
            println("Connecté à la machine" + Config.adresses[i] + " sur le port " + Config.ports[i]);
        }
    }

    private static void usage() {
        System.out.println("Usage: java HdfsClient read <file>");
        System.out.println("Usage: java HdfsClient write <line|kv> <file>");
        System.out.println("Usage: java HdfsClient delete <file>");
    }
	
    public static void HdfsDelete(String hdfsFname) {}
	
    public static void HdfsWrite(Format.Type fmt, String localFSSourceFname, 
     int repFactor) 
	{
	    // Récupérer le fichier, calculer le nombre de fragments.
	    // PATH, tab_serveurs sont des variables définies dans config/Parametres.java 
	    File f = new File (PATH + localFSSourceFname);
	    long taille_fichier = f.length;
	    int nombre_fragments = tab_serveurs.length;
	    
	    int taille_fragment = Math.ceil(taille_fichier / nombre_fragments);
	
	    if (fmt == Type.LINE)
	    {
                LineFormat fichier = new LineFormat(localFSSourceFname);
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
			c += kv.v.length;
		    }

		    // Envoyer le fragment i sur la machine i
		    Socket socket = new Socket (tab_serveurs[i], tab_ports[i]);
		    ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
		    // Commande : CMD_WRITE nom_fichier&LINE&texte_du_fragment
		    oos.writeObject("CMD_WRITE" + " " + localFSSourceFname + "&LINE&" + texte_fragment);
                    
		    oos.close();
                    socket.close();
	        }
		fichier.close();
            }
	    
	    else // fmt == Type.KV
	    {
	        KV fichier = new KVFormat(localFSSourceFname);
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
			c += kv.v.length;
		    }

		    // Envoyer le fragment i sur la machine i
		    Socket socket = new Socket (tab_serveurs[i], tab_ports[i]);
		    ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
		    // Commande : CMD_WRITE nom_fichier&KV&texte_du_fragment
		    oos.writeObject("CMD_WRITE" + " " + localFSSourceFname + "&KV&" + texte_fragment);
                    
		    oos.close();
                    socket.close();
		}
		fichier.close();
	    }
	}

    public static void HdfsRead(String hdfsFname, String localFSDestFname) { }

	
    public static void main(String[] args) {
        // java HdfsClient <read|write> <line|kv> <file>

        try {
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
                HdfsWrite(fmt,args[2],1);
            }	
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

}
