package hdfs;

import config.Config;
import formats.KVFormat;
import formats.LineFormat;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import formats.KV;
import formats.Format;

import java.net.Socket;

public class HdfsServer extends Thread {
	private Socket socket;
	int numero_serveur;
	Format file;
	Format.Type type_fichier;
	KV kv;
	static String[] PATH_SERVER = Config.PATH_SERVER;

	public HdfsServer(final Socket socket, int numero_serveur) {
		this.socket = socket;
		this.numero_serveur = numero_serveur;
	}

	public static void main(final String[] args) throws IOException {
		@SuppressWarnings("resource")
		final ServerSocket serverSocket = new ServerSocket(Config.tab_ports[Integer.parseInt(args[0])]);
		while (true) {
			System.out.println();
			System.out.println();
			System.out.println();
			System.out.println("Listening...");
			new HdfsServer(serverSocket.accept(), Integer.parseInt(args[0])).start();			
		}
	}

	@Override
	public void run() {
		try {
			int num = this.numero_serveur;
			System.out.println("Connexion réussie");
			// Transfert de donnees du client au serveur
			final ObjectInputStream objectInputStream = new ObjectInputStream(this.socket.getInputStream());
			
			System.out.println("test");
			// Transfert de données du serveur au client
			// final OutputStream outputOS = socket.getOutputStream();
			// On reçoit la commande à exécuter plus le nom de fragment
			final String[] split_commande = ((String) objectInputStream.readObject()).split("#");
			
			// Une commande est de la forme CMD_WRITE$nom_fichier&LINE&texte_du_fragment
			// On fait alors deux split successifs : l'un sur le type de la commande, l'autre sur les données fournies
			final String typeCommande = split_commande[0];
			final String fichier = split_commande[1];

			System.out.println("commande : " + typeCommande);
			switch (typeCommande) {
			case "CMD_WRITE":
				// On ouvre le fichier cree en mode ecriture afin d'y ecrire les parties du
				// fragment
				// On recoit le format du fichier du fragment à recevoir
				
				// Second split : nom_fichier&KV&clé1<->valeur1\ncle2<->valeur2\ncle3<->valeur3
				// On commence par séparer le nom du fichier, de son type, et des donnees
				final String[] splitFichier = fichier.split("&");
				final String fichier_a_ecrire = splitFichier[0];
				final String fmt_str = splitFichier[1];
				final Format.Type fmt = fmt_str == "LINE" ? Format.Type.LINE : Format.Type.KV;
				final String donnees = splitFichier[2];
				
				// Troisieme split : clé1<->valeur1\ncle2<->valeur2\ncle3<->valeur3
				// On sépare les KV entre eux avec les retours chariot, puis on reconstruit chaque KV
				final String[] tab_KV = donnees.split("\n");
				
				// On ouvre le fichier pour le préparer au travail
				if (fmt == Format.Type.KV)
					this.file = (Format) new KVFormat(PATH_SERVER[num]+ fichier_a_ecrire);
				else
					this.file = new LineFormat(PATH_SERVER[num] + fichier_a_ecrire);
				this.file.open(Format.OpenMode.W);
				
				for (String kv_str : tab_KV) {
					// On split les deux valeurs du KV : cle<->valeur
					String[] kv_split = kv_str.split("<->");
					// On recrée le KV reçu
					kv = new KV(kv_split[0], kv_split[1]);
					// On l'ajoute au fichier
					this.file.write(kv);
				}
					
				// BufferedWriter buff = new BufferedWriter(new OutputStreamWriter(outputOS));
				// System.out.println(fichier);
				// outputOS.write(1);
				this.file.close();
				objectInputStream.close();
				this.socket.close();
				break;
				
				
			case "CMD_READ":
				// Dans ce cas, une commande est CMD_READ nomFichier
				// Le split de départ suffit alors, avec fichier = nomFichier
				// Transfert de donnees du serveur au client (read)
				final ObjectOutputStream objectOutputStream = new ObjectOutputStream(this.socket.getOutputStream());
				Format file = new KVFormat(PATH_SERVER[num] + fichier);
				file.open(Format.OpenMode.R);
				
				KV kv_a_envoyer;
				
				// SI le nom du fichier finit par .kv : 
				// On renvoie chaque kv au client
				while ((kv_a_envoyer = file.read()) != null) {
					String kv_str = kv_a_envoyer.k + "<->" + kv_a_envoyer.v;
					System.out.println(kv_str);
					objectOutputStream.writeObject(kv_str);
				}
				
				// Sinon (format LINE) : on renvoie uniquement la valeur des kv (même boucle en enlevant kv.k + "<->") 
				
				
				objectOutputStream.writeObject(null);
				System.out.println("Fin de connexion");
				file.close();
				objectInputStream.close();
				objectOutputStream.close();
				break;
				
			case "CMD_DELETE":
				
				/* // Transfert de donnees du serveur au client (accuse de reception pour client de la suppression)
				final ObjectOutputStream objectOutputStream1 = new ObjectOutputStream(this.socket.getOutputStream());
				objectOutputStream1.writeObject(new File(PATH_SERVER[num]+fichier).delete());
				objectOutputStream1.close(); */
				File f = new File(PATH_SERVER[num]+fichier);
				f.delete();
				
				objectInputStream.close();
				break;
				
			default:
				System.out.println(typeCommande + " : commande inconnue");
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}
}
