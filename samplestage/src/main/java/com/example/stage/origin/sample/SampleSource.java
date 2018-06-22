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
    // Offsets can vary depending on the data source. Here we use an integer as an example only.
    long nextSourceOffset = 0;
    if (lastSourceOffset != null) {
      nextSourceOffset = Long.parseLong(lastSourceOffset);
    }

    int numRecords = 0;

    // TODO: As the developer, implement your logic that reads from a data source in this method.

    // Create records and add to batch. Records must have a string id. This can include the source offset
    // or other metadata to help uniquely identify the record itself.
    while (numRecords < maxBatchSize) {
      Record record = getContext().createRecord("some-id::" + nextSourceOffset);
      Map<String, Field> map = new HashMap<>();
      map.put("fieldName", Field.create("Some Value"));
      record.set(Field.create(map));
      batchMaker.addRecord(record);
      ++nextSourceOffset;
      ++numRecords;
    }

    return String.valueOf(nextSourceOffset);
  }

}
