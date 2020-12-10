// 
// Decompiled by Procyon v0.5.36
// 

package hdfs;

import config.Project;
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
	Format file;
	Format.Type fmt;
	KV kv;
	static String PATH = Project.PATH;
	static String DATA = Project.DATAN7;

	public HdfsServer(final Socket socket) {
		this.socket = socket;
	}

	public static void main(final String[] args) throws IOException {
		@SuppressWarnings("resource")
		final ServerSocket serverSocket = new ServerSocket(Project.ports[Integer.parseInt(args[0])]);
		while (true) {
			System.out.println();
			System.out.println();
			System.out.println();
			System.out.println("Listening...");
			new HdfsServer(serverSocket.accept()).start();			
		}
	}

	@Override
	public void run() {
		try {
			System.out.println("Connexion réussie");
			// Transfert de donnees du client au serveur
			final ObjectInputStream objectInputStream = new ObjectInputStream(this.socket.getInputStream());
			// Transfert de donn�es du serveur au client
			// final OutputStream outputOS = socket.getOutputStream();
			// On re�oit la commande � ex�cuter plus le nom de fragment
			final String[] split = ((String) objectInputStream.readObject()).split(" ");
			// Nom de fragment
			final String nomFichier = split[1];
			Commande commande;

			switch (split[0]) {
			case "CMD_WRITE":
				commande = Commande.CMD_WRITE;
				break;
			case "CMD_READ":
				commande = Commande.CMD_READ;
				break;
			default:
				commande = Commande.CMD_DELETE;
			}

			
			
			if (commande == Commande.CMD_WRITE) {

				// On ouvre le fichier cree en mode ecriture afin d'y ecrire les parties du
				// fragment
				// On recoit le format du fichier du fragment à recevoir
				fmt = (Format.Type) objectInputStream.readObject();
				if (fmt == Format.Type.KV)
					this.file = (Format) new KVFormat(PATH + DATA + nomFichier);
				else
					this.file = new LineFormat(PATH + DATA + nomFichier);
				this.file.open(Format.OpenMode.W);
				// On recoit le premier element du fragment sous forme de KV
				final String s2 = (String) objectInputStream.readObject();
				boolean estNul = false;
				KV kv = null;
				if (s2 == null) {
					estNul = true;
				} else {
					final String[] split2 = s2.split("<->");
					// On recree le KV recu
					kv = new KV(split2[0], split2[1]);
				}
				while (!estNul) {
					// On ecrit le KV recu dans le fichier
					this.file.write(kv);
					// On recoit toutes les parties du fragment

					try {
						final Object object = objectInputStream.readObject();
						final String[] split3 = ((String) object).split("<->");
						kv = new KV(split3[0], split3[1]);
					} catch (Exception e) {
						estNul = true;
					}
				}

				// BufferedWriter buff = new BufferedWriter(new OutputStreamWriter(outputOS));
				System.out.println(nomFichier);
				// outputOS.write(1);
				this.file.close();
				objectInputStream.close();
				this.socket.close();

			} else if (commande == Commande.CMD_READ) {
				// Transfert de donnees du serveur au client (read)
				final ObjectOutputStream objectOutputStream = new ObjectOutputStream(this.socket.getOutputStream());
				Format file = new KVFormat(PATH + DATA + nomFichier);
				file.open(Format.OpenMode.R);
				KV kv;

				while ((kv = file.read()) != null) {
					String objetKV = kv.k + "<->" + kv.v;
					System.out.println(objetKV);
					objectOutputStream.writeObject(objetKV);
				}
				objectOutputStream.writeObject(null);
				System.out.println("Fin de connexion");
				file.close();
				objectInputStream.close();
				objectOutputStream.close();

			} else {
				// Transfert de donnees du serveur au client (accuse de reception pour client de la suppression)
				final ObjectOutputStream objectOutputStream = new ObjectOutputStream(this.socket.getOutputStream());
				objectOutputStream.writeObject(new File(PATH+DATA+nomFichier).delete());
				objectInputStream.close();

			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}
}
