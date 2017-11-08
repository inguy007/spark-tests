package com.bits.dissertation.outlier.ingestion;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;

public class FileWatcher implements Runnable{

     String watchDirectory;
     
     public FileWatcher(String watchDirectory) {
          this.watchDirectory = watchDirectory;
     }

     @Override
     public void run() {
          try{
               WatchService watcher = FileSystems.getDefault().newWatchService();
               Path dir = Paths.get(watchDirectory);
               dir.register(watcher, StandardWatchEventKinds.ENTRY_CREATE);
               while (true) {
                    WatchKey key;
                    try {
                         key = watcher.take();
                    } catch (InterruptedException ex) {
                         return;
                    }

                    for (WatchEvent<?> event : key.pollEvents()) {
                         WatchEvent.Kind<?> kind = event.kind();
                         WatchEvent<Path> ev = (WatchEvent<Path>) event;
                         Path fileName = ev.context();
                         System.out.println(kind.name() + ": " + fileName);
                         if (kind == StandardWatchEventKinds.ENTRY_CREATE) {
                              System.out.println("New File available");
                         }
                    }

                    boolean valid = key.reset();
                    if (!valid) {
                         break;
                    }
               }               
          }catch(IOException e){
               System.err.println("Error while watching directory :"+watchDirectory);
               System.exit(0);
          }

     }

}
