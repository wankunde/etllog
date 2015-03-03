package com.giant.etllog;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_DELETE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;
import static java.nio.file.StandardWatchEventKinds.OVERFLOW;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 文件变化监控服务
 * 
 * @author wankun
 * @date 2014年5月28日
 * @version 1.0
 */
public class WatchDirService implements Runnable {

	private final static Logger logger = LoggerFactory.getLogger(WatchDirService.class);

	private WatchService watcher = null;
	private final Map<WatchKey, Path> keys;
	private final boolean recursive;
	private boolean trace = true;

	@SuppressWarnings("unchecked")
	static <T> WatchEvent<T> cast(WatchEvent<?> event) {
		return (WatchEvent<T>) event;
	}

	/**
	 * Register the given directory with the WatchService
	 */
	private void register(Path dir) throws IOException {
		WatchKey key = dir.register(watcher, ENTRY_CREATE, ENTRY_DELETE, ENTRY_MODIFY);
		if (trace) {
			Path prev = keys.get(key);
			if (prev == null) {
				logger.info("register: " + dir);
			} else {
				if (!dir.equals(prev)) {
					logger.info("update: " + prev + " -> " + dir);
				}
			}
		}
		keys.put(key, dir);
	}

	private void unRegister(Path dir) throws IOException {
		WatchKey key = dir.register(watcher);
		keys.remove(key);
	}

	/**
	 * Register the given directory, and all its sub-directories, with the
	 * WatchService.
	 */
	private void registerAll(final Path start) throws IOException {
		// register directory and sub-directories
		Files.walkFileTree(start, new SimpleFileVisitor<Path>() {

			@Override
			public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
				register(dir);
				return FileVisitResult.CONTINUE;
			}
		});
	}

	/**
	 * Creates a WatchService and registers the given directory
	 */
	public WatchDirService(Path dir, boolean recursive) {
		this.keys = new HashMap<WatchKey, Path>();
		this.recursive = recursive;
		try {
			this.watcher = FileSystems.getDefault().newWatchService();

			if (recursive) {
				registerAll(dir);
			} else {
				register(dir);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		// enable trace after initial registration
		this.trace = true;
	}

	public void run() {
		for (;;) {
			try {
				WatchKey key;
				try {
					key = watcher.take();
				} catch (InterruptedException x) {
					return;
				}

				Path dir = keys.get(key);
				if (dir == null) {
					logger.error("WatchKey not recognized!!");
					continue;
				}

				for (WatchEvent<?> event : key.pollEvents()) {
					WatchEvent.Kind kind = event.kind();

					// TBD - provide example of how OVERFLOW event is handled
					if (kind == OVERFLOW) {
						continue;
					}

					// Context for directory entry event is the file name of
					// entry
					WatchEvent<Path> ev = cast(event);
					Path name = ev.context();
					Path child = dir.resolve(name);

					// 进行事件处理
					logger.debug(event.kind().name() + ":" + child);
					if (!(name.toString().startsWith(".") && child != null)) {
						// 删除事件 1. 删除文件名对应的pos文件 2.如果是软连接被删除，需要删除对应的的文件监控
						if (kind == ENTRY_DELETE) {
							try {
								if (Files.isSymbolicLink(child))
									unRegister(child.toRealPath());
							} catch (IOException e) {
								e.printStackTrace();
							}
						}

						// 创建事件 1. 如果文件是软连接，添加对应文件的监控
						if (kind == ENTRY_CREATE) {
							try {
								// 如果是文件夹,则进行新的注册
								if (Files.isDirectory(child))
									register(child.toRealPath());

								// if directory is created, and watching
								// recursively, then register it and its
								// sub-directories
								if (recursive) {
									if (Files.isDirectory(child)) {
										registerAll(child.toRealPath());
									}
								}
							} catch (IOException e) {
								e.printStackTrace();
							}
						}

						if (kind == ENTRY_MODIFY) {
							EventService.putFile(child);
						}
					}
				}

				// reset key and remove from set if directory no longer
				// accessible
				boolean valid = key.reset();
				if (!valid) {
					keys.remove(key);
					logger.warn("移除监控key-->" + key.toString());
					if (keys.isEmpty()) {
						logger.error("key重置失败,可能导致无法继续接收数据 :  " + valid);
						break;
					}
				}
				Thread.currentThread().sleep(1000);
			} catch (Exception e) {
				logger.error("文件监控服务异常", e);
			}
		}
	}
}
