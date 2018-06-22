/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.example.stage.origin.sample;

import com.example.stage.lib.sample.Errors;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.LogCommand;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.api.errors.NoHeadException;
import org.eclipse.jgit.lib.Constants;
import org.eclipse.jgit.lib.PersonIdent;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevSort;
import org.eclipse.jgit.revwalk.RevWalk;
import org.eclipse.jgit.storage.file.FileRepositoryBuilder;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;


/**
 * This source is an example and does not actually read from anywhere.
 * It does however, generate generate a simple record with one field.
 */
public abstract class SampleSource extends BaseSource {

	
	 private static final Logger LOG = LoggerFactory.getLogger(SampleSource.class);	
	
  /**
   * Gives access to the UI configuration of the stage provided by the {@link SampleDSource} class.
   */
  public abstract String getConfig();

  
  private static final String PATH = "/home/parkash/dev/git_repos/spring-profiles";
  
  
  private Repository repository;
  private Git git;  
  
  
  @Override
  protected List<ConfigIssue> init() {
    // Validate configuration values and open any required resources.
    List<ConfigIssue> issues = super.init();

    FileRepositoryBuilder builder = new FileRepositoryBuilder();
    try {
      repository = builder
          .setGitDir(new File(PATH+"/.git"))
          .setMustExist(true)
          .build();
      git = new Git(repository);

      LOG.info("Connected to Git repository at {}", 
          repository.getDirectory().getAbsolutePath());
    } catch (IOException e) {
      LOG.error("Exception building Git repository", e);
      issues.add(
          getContext().createConfigIssue(
              Groups.SAMPLE.name(), "config", Errors.SAMPLE_00, e.getLocalizedMessage()
          )
      );
    }

    // If issues is not empty, the UI will inform the user of each configuration issue in the list.
    return issues;
  }

  /** {@inheritDoc} */
  @Override
  public void destroy() {
	  
	// Clean up JGit resources.
	  if (git != null) {
	    git.close();
	  }
	  if (repository != null) {
	    repository.close();
	  }
	  
	  
    // Clean up any open resources.
    super.destroy();
  }

  /** {@inheritDoc} */
  @Override
  public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
	  Iterable<RevCommit> commits;
	  try {
	    LogCommand cmd = git.log();
	    if (lastSourceOffset == null || lastSourceOffset.isEmpty()) {
	      // Get all commits
	      cmd.all();
	    } else {
	      // Get commits since last offset
	      cmd.addRange(repository.resolve(lastSourceOffset),
	          repository.resolve(Constants.HEAD));
	    }
	    commits = cmd.call();
	    // Want oldest commits first, so we tell JGit to reverse the
	    // default ordering
	    ((RevWalk)commits).sort(RevSort.REVERSE);
	  } catch (NoHeadException e) {
	    // No commits yet. Returning null will stop the pipeline,
	    // so return an empty string so we wait for commits
	    return "";
	  } catch (GitAPIException | IOException e) {
	    throw new StageException(Errors.SAMPLE_00, e);
	  }
	  
	  String nextSourceOffset = lastSourceOffset;

	// Create records and add to batch
	int numRecords = 0;
	Iterator<RevCommit> iter = commits.iterator();

	if (!iter.hasNext()) {
	  // No data to process, but don't tie up the app!
	  try {
	    Thread.sleep(1000);
	  } catch (InterruptedException e) {
	    LOG.error("Sleep interrupted", e);
	  }
	} else {
	  while (numRecords < maxBatchSize && iter.hasNext()) {
	    RevCommit commit = iter.next();
	    String hash = commit.getName();

	    // Records are identified by the commit hash
	    Record record = getContext().createRecord("hash::" + hash);

	    Map<String, Field> map = new HashMap<>();
	    map.put("hash", Field.create(hash));
	    map.put("time", Field.create(commit.getCommitTime()));
	    map.put("short_message", Field.create(commit.getShortMessage()));

	    PersonIdent committer = commit.getCommitterIdent();
	    Map<String, Field> committerMap = new HashMap<>();
	    committerMap.put("name", Field.create(committer.getName()));
	    committerMap.put("email", Field.create(committer.getEmailAddress()));
	    map.put("committer", Field.create(committerMap));

	    record.set(Field.create(map));

	    LOG.debug("Adding record for git commit {}: {}", hash, record);

	    batchMaker.addRecord(record);
	    nextSourceOffset = hash;
	    ++numRecords;
	  }
	}

	return nextSourceOffset;	  
  }

}
