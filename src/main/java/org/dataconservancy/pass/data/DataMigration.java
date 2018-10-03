package org.dataconservancy.pass.data;
import java.io.FileOutputStream;

import java.net.URI;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;

import org.dataconservancy.pass.client.PassClient;
import org.dataconservancy.pass.client.PassClientFactory;
import org.dataconservancy.pass.client.SubmissionStatusService;
import org.dataconservancy.pass.client.fedora.FedoraConfig;
import org.dataconservancy.pass.model.Funder;
import org.dataconservancy.pass.model.Grant;
import org.dataconservancy.pass.model.Repository;
import org.dataconservancy.pass.model.Submission;
import org.dataconservancy.pass.model.Submission.Source;
import org.dataconservancy.pass.model.SubmissionEvent;
import org.dataconservancy.pass.model.SubmissionEvent.EventType;
import org.dataconservancy.pass.model.SubmissionEvent.PerformerRole;
import org.dataconservancy.pass.model.User;
import org.dataconservancy.pass.model.support.Identifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Copyright 2018 Johns Hopkins University
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

/**
 * Data migration code
 * @author Karen Hanson
 */
public class DataMigration {

    private static final Logger LOG = LoggerFactory.getLogger(DataMigration.class);

    private final static String PASS_BASE_URL = "http://localhost:8080/fcrepo/rest/";
    private final static String PASS_ELASTICSEARCH_URL = "http://localhost:9200/pass/";
    private final static String PASS_FEDORA_USER = "fedoraAdmin";
    private final static String PASS_FEDORA_PASSWORD = "moo";
    private final static String PASS_SEARCH_LIMIT = "50000";

    private static final String DOMAIN = "johnshopkins.edu";
    private static final String EMPLOYEE_ID_TYPE = "employeeid";
    private static final String HOPKINS_ID_TYPE = "hopkinsid";
    private static final String JHED_ID_TYPE = "jhed";
    private static final String GRANT_ID_TYPE = "grant";
    private static final String FUNDER_ID_TYPE = "funder";

    private static int successfulSubmissions = 0;
    private static int unsuccessfulSubmissions = 0;
    private static int createdSubmissionEvents = 0;

    private static int successfulObjects = 0;
    private static int unsuccessfulObjects = 0;
    private static int skippedObjects = 0;

    private static final PassClient client = PassClientFactory.getPassClient(true);
    private static final org.dataconservancy.pass.v2_3.client.PassClient oldClient =
            org.dataconservancy.pass.v2_3.client.PassClientFactory.getPassClient();
    
    static {
        // Hardcoding these things in
        System.setProperty("pass.fedora.baseurl", PASS_BASE_URL);
        System.setProperty("pass.elasticsearch.url", PASS_ELASTICSEARCH_URL);
        System.setProperty("pass.fedora.user", PASS_FEDORA_USER);
        System.setProperty("pass.fedora.password", PASS_FEDORA_PASSWORD);
        System.setProperty("pass.elasticsearch.limit", PASS_SEARCH_LIMIT);
    }

    static final java.io.File dumpDir = new java.io.File("dump-" + new Date().getTime());

    static final java.io.File deletedDir = new java.io.File(dumpDir, "deleted");

    static final java.io.File editedDir = new java.io.File(dumpDir, "edited");
    
    static CloseableHttpClient http;

    
    public static void main(String[] args) {
        
        try {
            //include this block if you want to dump a copy of each deleted or edited record into a local folder.
            //for an example of how this is used, see delete consumer below
            /*
            http = getAuthClient();
            editedDir.mkdirs();
            deletedDir.mkdirs();
            System.out.println("Dumping deleted resources in " + deletedDir.getAbsolutePath());
            System.out.println("Dumping resources prior to editing in " + editedDir.getAbsolutePath());
            */
            
            migrateSubmissionModel();
            migrateRepository();
            migrateUsers();
            migrateGrants();
            migrateFunders();
                        
        } catch (Exception ex)  {
            System.err.println("Update failed: " + ex.getMessage());
        }
    }    
    
    private static void migrateSubmissionModel() {
        int recordsProcessed = client.processAllEntities(uri -> migrateSubmission(uri), Submission.class);
        
        LOG.info("********************************************************");
        LOG.info("Submission crawled: {}", recordsProcessed);
        LOG.info("Submissions successfully updated: {}", successfulSubmissions);
        LOG.info("Submissions with failed update: {}", unsuccessfulSubmissions);
        LOG.info("Submission Events created: {}", createdSubmissionEvents);
        LOG.info("********************************************************");
    }
    private static void migrateUsers() {
        successfulSubmissions = 0;
        unsuccessfulSubmissions = 0;
        skippedObjects = 0;
        int recordsProcessed =  client.processAllEntities(uri -> migrateUser(uri), User.class);

        LOG.info("********************************************************");
        LOG.info("Users crawled: {}", recordsProcessed);
        LOG.info("Users successfully updated: {}", successfulObjects);
        LOG.info("Users with failed update: {}", unsuccessfulObjects);
        LOG.info("Users skipped: {}", skippedObjects);
        LOG.info("********************************************************");
    }

    private static void migrateGrants() {
        successfulObjects = 0;
        unsuccessfulObjects = 0;
        skippedObjects = 0;
        int recordsProcessed = client.processAllEntities(uri -> migrateGrant(uri), Grant.class);

        LOG.info("********************************************************");
        LOG.info("Grants crawled: {}", recordsProcessed);
        LOG.info("Grants successfully updated: {}", successfulObjects);
        LOG.info("Grants with failed update: {}", unsuccessfulObjects);
        LOG.info("Grants skipped: {}", skippedObjects);
        LOG.info("********************************************************");
    }

    private static void migrateFunders() {
        successfulObjects = 0;
        unsuccessfulObjects = 0;
        skippedObjects = 0;
        int recordsProcessed = client.processAllEntities(uri -> migrateFunder(uri), Funder.class);

        LOG.info("********************************************************");
        LOG.info("Funders crawled: {}", recordsProcessed);
        LOG.info("Funders successfully updated: {}", successfulObjects);
        LOG.info("Funders with failed update: {}", unsuccessfulObjects);
        LOG.info("Funders skipped: {}", skippedObjects);
        LOG.info("********************************************************");
    }


    private static void migrateSubmission(URI uri) {
        
        try {
            Submission newSubmission = client.readResource(uri, Submission.class);
            org.dataconservancy.pass.v2_3.model.Submission origSubmission = oldClient.readResource(uri, org.dataconservancy.pass.v2_3.model.Submission.class);
            URI submitter = origSubmission.getUser();
            
            //If there is already a value in submitter and submissionStatus, don't need to redo
            if (newSubmission.getSubmitter()==null
                    || newSubmission.getSubmissionStatus()==null
                    || submitter!=null) {
                newSubmission.setSubmitter(submitter);
                SubmissionStatusService statusService = new SubmissionStatusService(newSubmission);
                newSubmission.setSubmissionStatus(statusService.calculateSubmissionStatus());
                client.updateResource(newSubmission);
                LOG.info("Submission:{} was updated. Submitter:{}, Status:{}", 
                         uri, newSubmission.getSubmitter(), newSubmission.getSubmissionStatus());
                successfulSubmissions = successfulSubmissions+1;
                if (newSubmission.getSource().equals(Source.PASS)
                        && newSubmission.getSubmitted()) {
                    // shouldn't really need this check, but put in as a small extra precaution
                    Map<String, Object> submEventSearch = new HashMap<String, Object>();
                    submEventSearch.put("eventType", "submitted");
                    submEventSearch.put("submission", uri);
                    Set<URI> events = client.findAllByAttributes(SubmissionEvent.class, submEventSearch);
                    if (events.size()==0) {
                        SubmissionEvent event = new SubmissionEvent();
                        event.setSubmission(uri);
                        event.setPerformedBy(submitter);
                        event.setEventType(EventType.SUBMITTED);
                        event.setPerformerRole(PerformerRole.SUBMITTER);
                        event.setPerformedDate(newSubmission.getSubmittedDate());
                        URI eventUri = client.createResource(event);
                        LOG.info("SubmissionEvent:{} was created for Submission {}", eventUri, uri);
                        createdSubmissionEvents = createdSubmissionEvents+1;
                    }
                }
            }            
        } catch (Exception ex) {
            LOG.error("Could not update Submission {}. Error mesage: {}", uri, ex.getMessage());
            unsuccessfulSubmissions = unsuccessfulSubmissions+1;
            //continue anyway
        }
       
    }
    private static void migrateUser(URI uri) {
        try {
            User newUser = client.readResource(uri, User.class);
            org.dataconservancy.pass.v2_3.model.User origUser = oldClient.readResource(uri, org.dataconservancy.pass.v2_3.model.User.class);
            boolean update = false;
            List<String> ids = new ArrayList<>();

            if (newUser.getLocatorIds().size() == 0) {
                if (origUser.getLocalKey() != null) {
                    ids.add(new Identifier(DOMAIN, EMPLOYEE_ID_TYPE, origUser.getLocalKey()).serialize());
                }
                if (origUser.getInstitutionalId() != null) {
                    String instId = origUser.getInstitutionalId().toLowerCase();
                    ids.add(new Identifier(DOMAIN, JHED_ID_TYPE, instId).serialize());
                }
                if (ids.size() > 0) {
                    newUser.setLocatorIds(ids);
                    update = true;
                }
            }

            if (newUser.getEmail() == null) {
                if (origUser.getEmail() != null) {
                    newUser.setEmail(origUser.getEmail());
                    update = true;
                }
            }

            if (newUser.getDisplayName() == null) {
                if (origUser.getFirstName() != null && origUser.getLastName() != null) {
                    newUser.setDisplayName(String.join(" ", origUser.getFirstName(), origUser.getLastName()));
                    update = true;
                }
            }

            if (update) {
                client.updateResource(newUser);
                successfulObjects++;
            } else {
                skippedObjects++;
            }
        } catch (Exception ex) {
            LOG.error("Could not update User {}. Error message: {}", uri, ex.getMessage());
            unsuccessfulObjects++;
        }
    }

    private static void migrateGrant(URI uri) {
        try {
            Grant grant = client.readResource(uri, Grant.class);
            if (!grant.getLocalKey().startsWith(DOMAIN)) {
                grant.setLocalKey(new Identifier(DOMAIN, GRANT_ID_TYPE, grant.getLocalKey()).serialize());
                client.updateResource(grant);
                successfulObjects++;
            } else {
                skippedObjects++;
            }
        } catch (Exception ex) {
            LOG.error("Could not update Grant {}. Error message: {}", uri, ex.getMessage());
            unsuccessfulObjects++;
        }
    }

    private static void migrateFunder(URI uri) {
        try {
            Funder funder = client.readResource(uri, Funder.class);
            if (!funder.getLocalKey().startsWith(DOMAIN)) {
                funder.setLocalKey(new Identifier(DOMAIN, FUNDER_ID_TYPE, funder.getLocalKey()).serialize());
                client.updateResource(funder);
                successfulObjects++;
            } else {
                skippedObjects++;
            }
        } catch (Exception ex) {
            LOG.error("Could not update Funder {}. Error message: {}", uri, ex.getMessage());
            unsuccessfulObjects++;
        }
    }

    private static void migrateRepository() {
        URI jscholarshipRepoUri;
        try {
            jscholarshipRepoUri = new URI("http://localhost:8080/fcrepo/rest/repositories/41/96/0a/92/41960a92-d3f8-4616-86a6-9e9cadc1a269");
            Repository repository = client.readResource(jscholarshipRepoUri, Repository.class);
            
            String formSchema = "{"  
                        + "\"id\":\"JScholarship\","
                        + "\"schema\":{"
                           + "\"title\":\"Johns Hopkins - JScholarship <br><p class='lead text-muted'>Deposit requirements for JH's institutional repository JScholarship.</p>\","
                           + "\"type\":\"object\","
                           + "\"properties\":{"
                              + "\"authors\":{"
                                 + "\"title\":\"<div class='row'><div class='col-6'>Author(s) <small class='text-muted'>(required)</small></div><div class='col-6 p-0'></div></div>\","
                                 + "\"type\":\"array\","
                                 + "\"uniqueItems\":true,"
                                 + "\"items\":{"
                                     + "\"type\":\"object\","
                                     + "\"properties\":{"
                                         + "\"author\":{"
                                             + "\"type\":\"string\","
                                             + "\"fieldClass\":\"body-text col-6 pull-left pl-0\""
                                       + "}"
                                    + "}"
                                 + "}"
                              + "}"
                           + "}"
                         + "},"
                        + "\"options\":{"
                           + "\"fields\":{"
                              + "\"authors\":{"
                                 + "\"hidden\":false"
                              + "}"
                           + "}"
                        + "}"
                     + "}";
            
            String agreementText = "NON-EXCLUSIVE LICENSE FOR USE OF MATERIALS This non-exclusive license defines the terms for the deposit of Materials in all formats "
                    + "into the digital repository of materials collected, preserved and made available through the Johns Hopkins Digital Repository, JScholarship. "
                    + "The Contributor hereby grants to Johns Hopkins a royalty free, non-exclusive worldwide license to use, re-use, display, distribute, transmit, "
                    + "publish, re-publish or copy the Materials, either digitally or in print, or in any other medium, now or hereafter known, for the purpose of including "
                    + "the Materials hereby licensed in the collection of materials in the Johns Hopkins Digital Repository for educational use worldwide. In some cases, access "
                    + "to content may be restricted according to provisions established in negotiation with the copyright holder. This license shall not authorize the commercial "
                    + "use of the Materials by Johns Hopkins or any other person or organization, but such Materials shall be restricted to non-profit educational use. Persons "
                    + "may apply for commercial use by contacting the copyright holder. Copyright and any other intellectual property right in or to the Materials shall not be "
                    + "transferred by this agreement and shall remain with the Contributor, or the Copyright holder if different from the Contributor. Other than this limited "
                    + "license, the Contributor or Copyright holder retains all rights, title, copyright and other interest in the images licensed. If the submission contains "
                    + "material for which the Contributor does not hold copyright, the Contributor represents that s/he has obtained the permission of the Copyright owner to "
                    + "grant Johns Hopkins the rights required by this license, and that such third-party owned material is clearly identified and acknowledged within the text "
                    + "or content of the submission. If the submission is based upon work that has been sponsored or supported by an agency or organization other than Johns Hopkins, "
                    + "the Contributor represents that s/he has fulfilled any right of review or other obligations required by such contract or agreement. Johns Hopkins will not "
                    + "make any alteration, other than as allowed by this license, to your submission. This agreement embodies the entire agreement of the parties. No modification "
                    + "of this agreement shall be of any effect unless it is made in writing and signed by all of the parties to the agreement.";
            
            repository.setAgreementText(agreementText);
            repository.setFormSchema(formSchema);
            client.updateResource(repository);
        } catch (Exception ex) {
            LOG.error("Could not update JScholarship agreementText", ex.getMessage());
            throw new RuntimeException(ex);
        }
        
    }
    
    private static Consumer<URI> delete = (id) -> {
        dump(deletedDir, id);
        client.deleteResource(id);
        System.out.println("Deleted resource with URI " + id.toString());
    };
    
    // This causes us to do another fetch of the resource content, but oh well
    private static void dump(java.io.File dir, URI uri) {
        final String path = uri.getPath();

        final java.io.File dumpfile = new java.io.File(dir, path + ".nt");
        dumpfile.getParentFile().mkdirs();

        final HttpGet get = new HttpGet(uri);
        get.setHeader("Accept", "application/n-triples");

        try (FileOutputStream out = new FileOutputStream(dumpfile);
                CloseableHttpResponse response = http.execute(get)) {

            response.getEntity().writeTo(out);

        } catch (final Exception e) {
            throw new RuntimeException("Error dumping contents of " + uri, e);
        }

    }

    static CloseableHttpClient getAuthClient() {
        final CredentialsProvider provider = new BasicCredentialsProvider();
        final UsernamePasswordCredentials credentials = new UsernamePasswordCredentials(FedoraConfig.getUserName(),
                FedoraConfig.getPassword());
        provider.setCredentials(AuthScope.ANY, credentials);

        return HttpClientBuilder.create()
                .setDefaultCredentialsProvider(provider)
                .build();
    }

    
}
