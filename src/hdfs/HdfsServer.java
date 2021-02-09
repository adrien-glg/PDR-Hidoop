package hdfs;

import config.Config;
import formats.KVFormat;
import formats.LineFormat;

import java.io.*;
import java.net.ServerSocket;
import formats.KV;
import formats.Format;
import java.util.concurrent.locks.*;


import java.nio.file.*;

import java.net.Socket;

public class HdfsServer extends Thread {
	private Socket socket;
	int numero_serveur;
	int numero_paquet;
	Format file;
	public static Object mutex=new Object();
	Format.Type type_fichier;
	KV kv;
	static String[] PATH_SERVER = Config.PATH_SERVER;
	

	public HdfsServer(final Socket socket, int numero_serveur) {
		this.socket = socket;
		this.numero_serveur = numero_serveur;
		this.numero_paquet = 0;
	}

	public static void main(final String[] args) throws IOException {
		// @SuppressWarnings("resource")
		final ServerSocket serverSocket = new ServerSocket(Config.tab_ports[Integer.parseInt(args[0])]);
		while (true) {
			System.out.println();
			System.out.println();
			System.out.println();
			System.out.println("Listening...");
			HdfsServer h = new HdfsServer(serverSocket.accept(), Integer.parseInt(args[0]));
			h.start();
			try{
				h.join();
			}
			catch (Exception e)
			{
				e.printStackTrace();
			}			
		}
	}

	@Override
	public void run() {
		
		try {
			
			int num = this.numero_serveur;
			System.out.println("Connexion réussie");
			// Transfert de donnees du client au serveur
			final ObjectInputStream ois = new ObjectInputStream(this.socket.getInputStream());
			
			// Transfert de données du serveur au client
			// final OutputStream outputOS = socket.getOutputStream();
			// On reçoit la commande à exécuter plus le nom de fragment
			
			
			synchronized(mutex){
			System.out.println("VERROUILLAGE");
			
			CommandeEtDonnees cd = (CommandeEtDonnees) ois.readObject();
			
			byte[] data = cd.donnees;
			String donnees = new String(data);
			String cmd_recue = cd.commande;
			
			//String chaine = new String(data);
			//System.out.println(chaine);
			final String[] split_commande = cmd_recue.split("#");
			
			// Une commande est de la forme CMD_WRITE#nom_fichier&LINE&texte_du_fragment
			// On fait alors deux split successifs : l'un sur le type de la commande, l'autre sur les données fournies
			final String typeCommande = split_commande[0];
			final String fichier_et_format = split_commande[1];

			//System.out.println("commande : " + typeCommande);
			switch (typeCommande) {
			case "CMD_WRITE":
				
				// On ouvre le fichier cree en mode ecriture afin d'y ecrire les parties du
				// fragment
				// On recoit le format du fichier du fragment à recevoir
				
				// Second split : nom_fichier&KV&clé1<->valeur1\ncle2<->valeur2\ncle3<->valeur3
				// On commence par séparer le nom du fichier, de son type, et des donnees
				final String[] splitFichier = fichier_et_format.split("&");
				//final int num_paq = Integer.parseInt(splitFichier[0]);
				final String fichier_a_ecrire = splitFichier[0];
				final String fmt_str = splitFichier[1];
				//System.out.println(fmt_str);
				
				FileOutputStream fos = new FileOutputStream(PATH_SERVER[num] + fichier_a_ecrire, true);

				final Format.Type fmt;
				
				if (fmt_str.equals("LINE")) 
				{
					//System.out.println("Line !!");
					fmt = Format.Type.LINE;
				} 
				else 
				{
					fmt = Format.Type.KV;
				}
				
				// Troisieme split : clé1<->valeur1\ncle2<->valeur2\ncle3<->valeur3
				// On sépare les KV entre eux avec les retours chariot, puis on reconstruit chaque KV				
				
				
		
					if (fmt == Format.Type.KV)
					{
						//System.out.println("kv");
						// On split les deux valeurs du KV : cle<->valeur
						String[] kv_split = donnees.split("<->");
						// On recrée le KV reçu
						kv = new KV(kv_split[0], kv_split[1]);
					}
					else
					{
						kv = new KV("", donnees);
					}
					// On l'ajoute au fichier
					// System.out.println("ecrit : "+ kv.v);
					
					
					
					
					try {
						fos.write(data);
					}
					catch (IOException e)
					{
						System.out.println("exception!");
						
					}
					
					
				// BufferedWriter buff = new BufferedWriter(new OutputStreamWriter(outputOS));
				// System.out.println(fichier);
				// outputOS.write(1);
				
			
				ois.close();
				this.socket.close();
				this.numero_paquet ++;
				fos.close();
				break;
				
				
			case "CMD_READ":
				Format file;
				// Dans ce cas, une commande est CMD_READ#nomFichier
				// Le split de départ suffit alors, avec fichier_et_format = nomFichier
				// Transfert de donnees du serveur au client (read)
				final ObjectOutputStream objectOutputStream = new ObjectOutputStream(this.socket.getOutputStream());
				System.out.println(PATH_SERVER[num] + fichier_et_format);
				
				String[] decoupe = fichier_et_format.split("\\.");
				String format_str = decoupe[decoupe.length - 1];
				
				System.out.println(format_str);
				
				if (format_str.equals("kv"))
				{
					file = new KVFormat(PATH_SERVER[num] + fichier_et_format);
				}
				else 
				{
					file = new LineFormat(PATH_SERVER[num] + fichier_et_format);
				}
				file.open(Format.OpenMode.R);
				
				KV kv_a_envoyer;
				
				// SI le nom du fichier finit par .kv : 
				
				if (format_str.equals("kv"))
				{
					// On renvoie chaque kv au client
					while ((kv_a_envoyer = file.read()) != null) {
						String kv_str = kv_a_envoyer.k + "<->" + kv_a_envoyer.v;
						System.out.println(kv_str);
						objectOutputStream.writeObject(kv_str);
					}
				}
				
				
				// Sinon (format LINE) : on renvoie uniquement la valeur des kv
				else
				{
					while ((kv_a_envoyer = file.read()) != null) {
						String kv_str = kv_a_envoyer.v;
						System.out.println(kv_str);
						objectOutputStream.writeObject(kv_str);
					}
				}
				
				objectOutputStream.writeObject(null);
				System.out.println("Fin de connexion");
				file.close();
				ois.close();
				objectOutputStream.close();
				break;
				
			case "CMD_DELETE":
				
				/* // Transfert de donnees du serveur au client (accuse de reception pour client de la suppression)
				final ObjectOutputStream objectOutputStream1 = new ObjectOutputStream(this.socket.getOutputStream());
				objectOutputStream1.writeObject(new File(PATH_SERVER[num]+fichier).delete());
				objectOutputStream1.close(); */
				File f = new File(PATH_SERVER[num]+fichier_et_format);
				f.delete();
				
				ois.close();
				break;
				
			default:
				System.out.println(typeCommande + " : commande inconnue");
			}
			
		}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}
}
