/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.addthis.meshy.service.file;

import com.addthis.basis.util.LessBytes;
import com.addthis.basis.util.Parameter;
import com.addthis.meshy.*;
import com.google.common.base.Objects;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static com.google.common.base.Preconditions.checkState;

public class FileSource extends SourceHandler {
    static final Logger log = LoggerFactory.getLogger(FileSource.class);

    private static final int FILE_FIND_WINDOW_SIZE = Parameter.intValue("meshy.finder.window", 50_000);

    // not thread safe, and only used for single-channel cases (eg. clients)
    private final LinkedList<FileReference> list = new LinkedList<>();
    private List<String> fileRequest;
    private FileReferenceFilter filter;
    private long currentWindow = 0;

    public FileSource(ChannelMaster master) {
        super(master, FileTarget.class, true);
    }

    public FileSource(ChannelMaster master, String[] files) {
        this(master);
        requestRemoteFiles(files);
    }

    public FileSource(ChannelMaster master, String[] files, String scope) {
        this(master);
        requestFiles(scope, files);
    }

    public FileSource(ChannelMaster master, String[] files, FileReferenceFilter filter) {
        this(master);
        this.filter = filter;
        requestRemoteFiles(files);
    }

    public void requestRemoteFiles(String... matches) {
        requestFiles("local", matches);
    }

    public void requestRemoteFilesWithUpdates(String... matches) {
        requestFiles("localF", matches);
    }

    public void requestLocalFiles(String... matches) {
        start(MeshyConstants.LINK_NAMED);
        requestFilesPostStart("remote", matches);
    }

    private void requestFiles(String scope, String... matches) {
        start();
        requestFilesPostStart(scope, matches);
    }

    private void requestFilesPostStart(String scope, String... matches) {
        checkState(fileRequest == null, "file search request already started");
        this.fileRequest = Arrays.asList(matches);
        send(LessBytes.toBytes(scope));
        log.debug("{} scope={}", this, scope);
        for (String match : matches) {
            log.trace("{} request={}", this, match);
            send(LessBytes.toBytes(match));
        }
        send(new byte[]{-1});
        sendInitialWindowing();
    }

    void sendInitialWindowing() {
        increaseClientWindow(FILE_FIND_WINDOW_SIZE);
    }

    private void increaseClientWindow(int windowSize) {
        this.currentWindow += windowSize;
        send(LessBytes.toBytes(windowSize));
    }

    public Collection<FileReference> getFileList() {
        return list;
    }

    public Map<String, FileReference> getFileMap() {
        HashMap<String, FileReference> map = new HashMap<>();
        for (FileReference file : getFileList()) {
            map.put(file.name, file);
        }
        return map;
    }

    @Override
    public void receive(ChannelState state, int length, ByteBuf buffer) throws Exception {
        currentWindow -= 1;
        if (currentWindow <= (FILE_FIND_WINDOW_SIZE / 2)) {
            increaseClientWindow(FILE_FIND_WINDOW_SIZE / 2);
        }
        /* sync not required b/c overridden in server-server calls */
        FileReference ref = new FileReference(Meshy.getBytes(length, buffer));
        if (filter == null || filter.accept(ref)) {
            receiveReference(ref);
        }
        log.trace("{} recv={}", this, list.size());
    }

    @Override
    public void receiveComplete(ChannelState state, int completedSession) throws Exception {
        log.debug("recv.complete [{}] {} from {}", completedSession, fileRequest, state.getName());
        super.receiveComplete(state, completedSession);
    }

    // override to detect unexpected channel closures
    @Override
    public void channelClosed(ChannelState state) {
    }

    // override in subclasses for async handling
    // call super() if you still want the list populated
    public void receiveReference(FileReference ref) {
        list.add(ref);
    }

    // override in subclasses for async handling
    @Override
    public void receiveComplete() throws Exception {
        log.debug("{} recvComplete", this);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("fileRequest", fileRequest)
                .add("filter", filter)
                .toString();
    }
}
