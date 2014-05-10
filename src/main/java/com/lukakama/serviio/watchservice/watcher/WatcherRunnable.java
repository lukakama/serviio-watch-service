/*
 * Copyright 2014 Luca De Petrillo
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.lukakama.serviio.watchservice.watcher;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.channels.FileChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardOpenOption;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.IOUtils;
import org.serviio.library.entities.Repository;
import org.serviio.library.local.LibraryManager;
import org.serviio.library.local.service.RepositoryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WatcherRunnable implements Runnable {
	private static final Logger log = LoggerFactory.getLogger(WatcherRunnable.class);

	private final long CHECK_TIMEOUT = 5000;

	private WatchService watcher = null;

	private TrackingFileVisitor trackingFileVisitor = new TrackingFileVisitor();

	private Set<Path> unaccessiblePaths = new HashSet<Path>();
	private Set<Path> creatingPaths = new HashSet<Path>();
	private Set<Path> modifyingPaths = new HashSet<Path>();

	private long updateNotificationTimestamp = -1;

	private Object configChangeFlag;

	private void initialize() throws InterruptedException {
		try {
			watcher = FileSystems.getDefault().newWatchService();
			log.debug("WatchService implementation: {}", watcher.getClass().getName());
		} catch (IOException e) {
			throw new RuntimeException(
					"Unable to retrieve a WatchService instance. Monitoring can't be used in the current JVM.", e);
		}

		// Start monitoring repositories folder.
		LibraryManager libraryManager = LibraryManager.getInstance();
		List<Repository> repositories = RepositoryService.getAllRepositories();
		for (Repository repository : repositories) {
			checkInterrupted();
			if (libraryManager.isRepositoryUpdatable(repository)) {
				Path path = repository.getFolder().toPath();
				if (isPathFullyAccessible(path)) {
					log.info("Watching root path: {}", path);
					checkNewPath(path);
				} else {
					log.warn("Skipping unaccessible root path: {}", path);
				}
			}
		}
	}

	@Override
	public void run() {
		try {
			log.info("Watcher started.");

			initialize();

			while (true) {
				checkInterrupted();

				WatchKey watchKey = watcher.poll(CHECK_TIMEOUT, TimeUnit.MILLISECONDS);

				if (watchKey != null) {
					try {
						log.debug("Received watchKey: {} - {}", watchKey, watchKey.watchable());

						Path parentPath = (Path) watchKey.watchable();

						for (WatchEvent<?> event : watchKey.pollEvents()) {
							checkInterrupted();

							log.debug("Received event: {} - {}", event.kind(), event.context());

							if (event.kind() == StandardWatchEventKinds.OVERFLOW) {
								log.warn("Performing a full scan due loss of native FileSystem tracking data.");

								// Canceling the old watcher.
								IOUtils.closeQuietly(watcher);

								// Clean any pending unaccessible path.
								unaccessiblePaths.clear();
								creatingPaths.clear();
								modifyingPaths.clear();

								// Re-initialize the monitoring.
								initialize();

							} else {
								Path eventPath = parentPath.resolve((Path) event.context());

								if (event.kind() == StandardWatchEventKinds.ENTRY_CREATE) {
									checkNewPath(eventPath);

								} else if (event.kind() == StandardWatchEventKinds.ENTRY_MODIFY) {
									if (!unaccessiblePaths.contains(eventPath)) {
										if (isPathFullyAccessible(eventPath)) {
											handlePathChanged(eventPath);

										} else {
											unaccessiblePaths.add(eventPath);
											modifyingPaths.add(eventPath);

											log.debug("File unacessible upon modification. Starting monitoring for '{}'.",
													eventPath);
										}
									}
								} else if (event.kind() == StandardWatchEventKinds.ENTRY_DELETE) {
									if (unaccessiblePaths.contains(eventPath)) {
										unaccessiblePaths.remove(eventPath);

										log.debug("Monitored file removed. Canceling monitoring for '{}'.", eventPath);

										if (modifyingPaths.contains(eventPath)) {
											modifyingPaths.remove(eventPath);
											handlePathRemoved(eventPath);
										} else {
											creatingPaths.remove(eventPath);
										}
									} else {
										handlePathRemoved(eventPath);
									}
								}
							}
						}

					} finally {
						watchKey.reset();
					}
				}

				if (!unaccessiblePaths.isEmpty()) {
					checkAccessiblePaths();

				} else if ((updateNotificationTimestamp != -1)
						&& ((System.currentTimeMillis() - updateNotificationTimestamp) > CHECK_TIMEOUT)) {
					// Nothing happened since last update. Resetting the updateTimestamp and requesting a library update on
					// Serviio.
					updateNotificationTimestamp = -1;
					updateServiioReposotories();

				} else if (updateNotificationTimestamp == -1) {
					// No pending path to checks and no pending updates to notify. Check external configuration changes.

					Object newFlagValue = getFieldValue(LibraryManager.getInstance(), "libraryAdditionsCheckerThread");
					if ((configChangeFlag != null) && (newFlagValue != configChangeFlag)) {
						log.info("Detected configuration change. Restart monitoring.");
						configChangeFlag = newFlagValue;
						
						// Canceling the old watcher.
						IOUtils.closeQuietly(watcher);

						// Clean any pending unaccessible path.
						unaccessiblePaths.clear();
						creatingPaths.clear();
						modifyingPaths.clear();

						// Re-initialize the monitoring.
						initialize();
					}
				}
			}
		} catch (InterruptedException e) {
			// This thread has been interrupted. Just exit.
			return;

		} finally {
			// Release any bounded resource.
			unaccessiblePaths.clear();
			creatingPaths.clear();
			modifyingPaths.clear();

			IOUtils.closeQuietly(watcher);

			log.info("Watcher stopped.");
		}
	}

	private void updateServiioReposotories() {
		log.info("Forcing Serviio's libraries update.");
		LibraryManager libraryManager = LibraryManager.getInstance();
		libraryManager.stopLibraryAdditionsCheckerThread();
		libraryManager.stopLibraryUpdatesCheckerThread();

		libraryManager.startLibraryAdditionsCheckerThread();
		libraryManager.startLibraryUpdatesCheckerThread();

		// Update the config change flag.
		configChangeFlag = getFieldValue(LibraryManager.getInstance(), "libraryAdditionsCheckerThread");
	}

	private void checkNewPath(Path path) throws InterruptedException {
		if (isPathFullyAccessible(path)) {
			handlePathNew(path);

		} else {
			unaccessiblePaths.add(path);
			creatingPaths.add(path);

			log.debug("File unacessible upon creation. Starting monitoring for '{}'.", path);
		}
	}

	private void checkAccessiblePaths() throws InterruptedException {
		log.debug("Checking for accessible paths.");

		for (Iterator<Path> itUnacessiblePaths = unaccessiblePaths.iterator(); itUnacessiblePaths.hasNext();) {
			checkInterrupted();

			Path pathToCheck = itUnacessiblePaths.next();

			if (isPathFullyAccessible(pathToCheck)) {
				// Path is fully accessible.
				log.debug("Accessible: '{}'.", pathToCheck);

				itUnacessiblePaths.remove();

				if (modifyingPaths.contains(pathToCheck)) {
					modifyingPaths.remove(pathToCheck);
					handlePathChanged(pathToCheck);
				} else {
					creatingPaths.remove(pathToCheck);
					handlePathNew(pathToCheck);
				}

			} else if (!Files.exists(pathToCheck)) {
				// File removed.
				log.debug("Unacessible file removed: '{}'.", pathToCheck);
				itUnacessiblePaths.remove();

				if (modifyingPaths.contains(pathToCheck)) {
					modifyingPaths.remove(pathToCheck);
					handlePathRemoved(pathToCheck);
				} else {
					creatingPaths.remove(pathToCheck);
				}

			} else {
				// File exists but not fully accessible. Check again later.
				log.debug("Unacessible (deplay): '{}'.", pathToCheck);
			}
		}
	}

	private void handlePathNew(Path path) throws InterruptedException {
		if (Files.isDirectory(path)) {
			log.debug("Detected new directory: {}", path);
			updateNotificationTimestamp = System.currentTimeMillis();

			try {
				Files.walkFileTree(path, trackingFileVisitor);

				checkInterrupted();
			} catch (IOException e) {
				// All IOException should be handled by the trackingFileVisitor instance. If there are IOException thrown 
				// here, than we have a bug.
				throw new RuntimeException("Unhandled IOException", e);
			}
		} else {
			log.debug("Detected new file: {}", path);
			updateNotificationTimestamp = System.currentTimeMillis();
		}
	}

	private void handlePathChanged(Path path) {
		if (!Files.isDirectory(path)) {
			log.debug("Detected changed file: {}", path);
			updateNotificationTimestamp = System.currentTimeMillis();
		} else {
			log.debug("Detected changed directory: {}", path);
			updateNotificationTimestamp = System.currentTimeMillis();
		}
	}

	private void handlePathRemoved(Path path) {
		if (!Files.isDirectory(path)) {
			log.debug("Detected removed file: {}", path);
			updateNotificationTimestamp = System.currentTimeMillis();
		} else {
			log.debug("Detected removed directory: {}", path);
			updateNotificationTimestamp = System.currentTimeMillis();
		}
	}

	private void checkInterrupted() throws InterruptedException {
		if (Thread.interrupted()) {
			throw new InterruptedException();
		}
	}

	private boolean isPathFullyAccessible(Path path) {
		if (!Files.exists(path)) {
			return false;
		}

		if (Files.isDirectory(path)) {
			DirectoryStream<Path> directoryStream = null;
			try {
				directoryStream = Files.newDirectoryStream(path);

				return true;
			} catch (IOException e) {
				log.debug("Unaccessible directory: {}", path, e);
				return false;
			} finally {
				IOUtils.closeQuietly(directoryStream);
			}

		} else {
			FileChannel fileChannel = null;
			try {
				fileChannel = FileChannel.open(path, StandardOpenOption.READ);
				fileChannel.position(Files.size(path));
				
				return true;
			} catch (IOException e) {
				log.debug("Unaccessible file: {}", path, e);
				return false;
			} finally {
				IOUtils.closeQuietly(fileChannel);
			}
		}
	}

	private class TrackingFileVisitor extends SimpleFileVisitor<Path> {
		@Override
		public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
			if (Thread.interrupted()) {
				return FileVisitResult.TERMINATE;
			}

			log.debug("Pre-visiting directory: {}", dir);

			dir.register(watcher, StandardWatchEventKinds.ENTRY_CREATE, StandardWatchEventKinds.ENTRY_MODIFY,
					StandardWatchEventKinds.ENTRY_DELETE, StandardWatchEventKinds.OVERFLOW);

			return FileVisitResult.CONTINUE;
		}

		@Override
		public FileVisitResult visitFile(Path path, BasicFileAttributes attrs) throws IOException {
			if (Thread.interrupted()) {
				return FileVisitResult.TERMINATE;
			}

			if (!Files.isDirectory(path)) {
				log.debug("Visiting file: {}", path);

				try {
					checkNewPath(path);
				} catch (InterruptedException e) {
					return FileVisitResult.TERMINATE;
				}
			}

			return FileVisitResult.CONTINUE;
		}

		@Override
		public FileVisitResult visitFileFailed(Path path, IOException exc) throws IOException {
			if (Thread.interrupted()) {
				return FileVisitResult.TERMINATE;
			}
			
			log.debug("Error visiting path: {}", path, exc);
			

			// Handle the path in deferred way, or skip if unreadable.
			if (Files.isReadable(path)) {
				log.debug("Access error while walking the file tree. Scheduling a delayed check for: {}", path);

				unaccessiblePaths.add(path);
				creatingPaths.add(path);

			} else {
				log.warn("Skipping unreadable path: {}", path);
			}

			return FileVisitResult.CONTINUE;
		}
	}

	private static Object getFieldValue(Object obj, String fieldName) {
		try {
			Object iWantThis;

			Field f = obj.getClass().getDeclaredField(fieldName); //NoSuchFieldException
			if (!f.isAccessible()) {
				f.setAccessible(true);
				iWantThis = f.get(obj);
				f.setAccessible(false);
			} else {
				iWantThis = f.get(obj);
			}
			return iWantThis;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}
}
