package org.bloomreach.cms.script;

import javax.jcr.Node;
import javax.jcr.NodeIterator;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.query.Query;

import org.hippoecm.repository.HippoRepository;
import org.hippoecm.repository.HippoRepositoryFactory;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//import org.bloomreach.cms.scripts.LandingPage2XPageConversionScriptGroovy;

@Ignore
public class ScriptTester {

    private static final Logger log = LoggerFactory.getLogger(ScriptTester.class);

    @Test
    public void testReset() throws RepositoryException {
        HippoRepository repository = HippoRepositoryFactory.getHippoRepository("rmi://localhost:1099/hipporepository");
        Session session = repository.login("admin", "admin".toCharArray());

        final NodeIterator it = session.getWorkspace().getQueryManager().createQuery("content/documents//element(*,hst:xpagemixin)", Query.XPATH).execute().getNodes();
        while (it.hasNext()) {
            final Node node = it.nextNode();
            if (node.hasNode("hst:xpage")) {
                node.getNode("hst:xpage").remove();
            }
            node.removeMixin("hst:xpagemixin");
        }

        final Node myProject = session.getNode("/hst:myproject/hst:configurations/myproject");
        final Node preview = session.nodeExists("/hst:myproject/hst:configurations/myproject-preview") ? session.getNode("/hst:myproject/hst:configurations/myproject-preview") : null;

        if (preview != null) {
            preview.remove();
        }

        if (myProject.hasNode("hst:sitemap/_default_")) {
            myProject.getNode("hst:sitemap/_default_").remove();
        }
        if (myProject.hasNode("hst:xpages")) {
            myProject.getNode("hst:xpages").remove();
        }
        final NodeIterator items = myProject.getNode("hst:workspace/hst:sitemap").getNodes();
        while (items.hasNext()) {
            items.nextNode().remove();
        }

        final Node siteBaseNode = session.getNode("/content/documents/myproject");
        if (siteBaseNode != null && siteBaseNode.hasNode("xpages")) {
            siteBaseNode.getNode("xpages").remove();
        }


        session.save();
    }


//    @Test
//    public void testLandingPage2XPageConversionScript() throws RepositoryException {
//        HippoRepository repository = HippoRepositoryFactory.getHippoRepository("rmi://localhost:1099/hipporepository");
//        Session session = repository.login("admin", "admin".toCharArray());
//
//        LandingPage2XPageConversionScriptGroovy script = new LandingPage2XPageConversionScriptGroovy();
//        script.setLogger(log);
//        script.setParametersMap(ImmutableMap.of("hstConfiguration", "myproject", "xPageFolder", "xpages", "defaultXpageDocType", "myproject:bannerdocument"));
//        script.initialize(session);
//        final Node node = session.getNode("/hst:myproject/hst:configurations");
//        final boolean saveSession = script.doUpdate(node);
//        if (saveSession) {
//            session.save();
//        }
//    }


}
