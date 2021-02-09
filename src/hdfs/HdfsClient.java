/* une PROPOSITION de squelette, incomplète et adaptable... */

package hdfs;
import formats.Format;
import formats.KV;
import formats.KVFormat;
import formats.LineFormat;
import config.Config;
import java.io.*;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Arrays;


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
				oos.writeObject(new CommandeEtDonnees("CMD_DELETE#" + hdfsFname, new byte[0]));
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
		int taille_paquet = 512; // en octets, 10485760 pour 10 Mio
		
	    // Récupérer le fichier, calculer le nombre de fragments.
	    // PATH, tab_serveurs sont des variables définies dans config/Parametres.java 
	    File f = new File (Config.PATH + localFSSourceFname);
	    long taille_fichier = f.length(); // toujours en octets
	    int nombre_fragments = Config.tab_serveurs.length;
	    
	    int taille_fragment = (int) Math.ceil(taille_fichier / nombre_fragments);
	    
	    
		int nombre_paquets = (int) Math.ceil((double) taille_fichier / taille_paquet);
		
		
		System.out.println("La taille du fichier est " + String.valueOf(taille_fichier) + ".Il y a " + String.valueOf(nombre_fragments) + " fragments, " + String.valueOf(nombre_paquets) + " paquets de 512 octets. La taille d'un fragment est donc de " + String.valueOf(taille_fragment) + " octets et il y a " + String.valueOf(nombre_paquets) + " paquets");
	    if (fmt == Format.Type.LINE)
	    {
			File fichier;
            // LineFormat fichier = new LineFormat(Config.PATH + localFSSourceFname);
            try {
					fichier = new File(Config.PATH + localFSSourceFname);
				
					Socket socket;
					ObjectOutputStream oos;
					InputStream is = new FileInputStream(f);
					for (int i = 0 ; i < nombre_paquets ; i++)
					{
						byte[] data = new byte[taille_paquet];
						String commande = "CMD_WRITE#" + localFSSourceFname + "&LINE";
															
						// Lire le fichier dans ce qui reste de data
						is.read(data);
						
						CommandeEtDonnees cd = new CommandeEtDonnees(commande, data);
					
						// Envoyer le paquet i sur la machine correspondante
			    
						int numero_machine = (int) (nombre_fragments * (double) i/nombre_paquets);
						System.out.println("J'envoie le paquet " + String.valueOf(i) + " sur la machine " + String.valueOf(numero_machine));

						try {
							socket = new Socket (Config.tab_serveurs[numero_machine], Config.tab_ports[numero_machine]);
							oos = new ObjectOutputStream(socket.getOutputStream());
							// Commande : CMD_WRITE nom_fichier&LINE&texte_du_fragment
							//System.out.println("CMD_WRITE#" + localFSSourceFname + "&LINE&" + texte_fragment);
							oos.writeObject(cd);
							System.out.println("Socket envoyé vers la machine : " + numero_machine);
							oos.close();
							socket.close();

						} 
						catch (IOException e) {
							e.printStackTrace();
						}    
					} 
					is.close();
				}
			catch (FileNotFoundException e) {
				System.out.println("Fichier non trouvé.");
			}
			catch (Exception e) {
				e.printStackTrace();
			}
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
				    oos.writeObject("CMD_WRITE#" + " " + localFSSourceFname + "&KV&" + texte_fragment);
				    oos.close();
		            socket.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			fichier.close();
	    }
	}

    public static void HdfsRead(String hdfsFname) throws UnknownHostException, IOException, ClassNotFoundException {
		
		String[] decoupe = hdfsFname.split("\\.");
		String format_str = decoupe[decoupe.length - 1];
				
    	int n = Config.tab_serveurs.length;
    	Socket[] sockets = new Socket[n]; 
    	
        /*Boucle initialisant chaque socket et réalisant une démande de connexion*/
        for (int i = 0; i<n; i++){
            sockets[i] = new Socket(Config.tab_serveurs[i],Config.tab_ports[i]);
            System.out.println("Connecté à la machine" + Config.tab_serveurs[i] + " sur le port " + Config.tab_ports[i]);
        }
        
        /*Déclaraiton des OutputStream qui serviront pour envoyer la commande aux nodes*/ 
        ObjectOutputStream[] oOutputs = new ObjectOutputStream[n];
        
        /*Déclaraiton des InputStream qui serviront pour recevoir les fragments */
        ObjectInputStream[] oInputs = new ObjectInputStream[n];
        
        /*Definition du message qui va être envoyé par le socket*/
        CommandeEtDonnees commande = new CommandeEtDonnees("CMD_READ#" + hdfsFname, new byte[0]);
         
        /*Ouverture du fichier où on va écrire*/
        Format fichier = (Format) new KVFormat(Config.PATH+hdfsFname);
        fichier.open(KVFormat.OpenMode.W);
        
        /*Envoi des commandes sur les sockets et initialisation*/
        for (int i= 0;i<n;i++){
			System.out.println("Récupération fichiers du serveur " + i);
            oOutputs[i] = new ObjectOutputStream(sockets[i].getOutputStream());
            
            System.out.println("socket output ok");
            
            /*Écriture de la commande dans le outputStream*/ 
            
            System.out.println("Envoi de la commande " + commande);
            oOutputs[i].writeObject(commande);
            
            oInputs[i] = new ObjectInputStream(sockets[i].getInputStream());
            /*Réception du premier kv qui nous indique si le fichier est présent ds le serveur*/
            String KVs = (String) oInputs[i].readObject();
            System.out.println("J'ai reçu le fichier : " + KVs);
            
            Boolean kvNull = KVs == null;
            /*Initialisation de la version classe du KV*/ 
            KV kv = null;
            String[] k_et_s;
			if (kvNull) {
				/*Si le premier KV est nul alors on sait que le serveur n'a pu envoyer aucun KV donc il a pas le fragment*/ 
                System.out.println("Aucun fragment du fichier " + hdfsFname + " trouvé dans le serveur " + i);	
			} else {
				
				if (format_str.equals("kv"))
				{
					k_et_s = KVs.split("<->");
					kv = new KV(k_et_s[0], k_et_s[1]);
				}
				else
				{
					kv = new KV("", KVs);
				}
				
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
                    if (format_str.equals("kv"))
					{
						k_et_s = KVs.split("<->");
						kv = new KV(k_et_s[0], k_et_s[1]);
					}
					else
					{
						kv = new KV("", KVs);
					}
                    
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
              case "read": HdfsRead(args[1]); break;
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
