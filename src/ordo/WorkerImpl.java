package ordo;
import map.Mapper;
import map.Reducer;

import java.net.MalformedURLException;
import java.rmi.*;
import java.rmi.registry.LocateRegistry;

import formats.Format;

public class WorkerImpl implements Worker {
    
    public WorkerImpl() {
    }
    
    public class doMap implements Runnable {
    	
    	Mapper m;
        Format reader;
        Format writer;
        CallBack cb;
        
        public doMap(Mapper m, Format reader, Format writer, CallBack cb) {
        	this.m = m;
        	this.reader = reader;
        	this.writer = writer;
        	this.cb = cb;
        }
		@Override
		public void run() {
			this.reader.open(Format.OpenMode.R);
			this.writer.open(Format.OpenMode.W);
			this.m.map(this.reader, this.writer);
            
            //Envoyer le callback qui previent du map fini
			//TODO
			this.cb.incr();
			
			this.reader.close();
			this.writer.close();
            
		}
    	
    }
    public void runMap(Mapper m, Format reader, Format writer, CallBack cb) throws RemoteException {
        
        Thread t = new Thread(new doMap(m, reader, writer, cb));
        // Lancement thread
        t.start();
    }
    
    public static void main(String args[]) throws RemoteException, MalformedURLException {
    	int port = 1234;
    	LocateRegistry.createRegistry(port);
    	Naming.rebind("ADRESSE A DEFINIR!!!!", new WorkerImpl());
    }
}