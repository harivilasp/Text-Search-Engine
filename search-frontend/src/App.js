import "./App.css";
import { Layout, Menu, Breadcrumb, BackTop, Collapse } from "antd";
import SearchBar from "./components/SearchBar";
const { Header, Content, Footer } = Layout;
const { Panel } = Collapse;

const App = () => {
  return (
    <>
      <Layout className="layout">
        <Header>
          <div className="logo-header">SearchIN</div>
        </Header>
        <BackTop />
        <Content style={{ padding: "0 50px", marginTop: "30px" }}>
          <SearchBar />
        </Content>
        <Footer style={{ textAlign: "center" }}>
          ©2021 Developed by Hemanth Nhs
        </Footer>
      </Layout>
    </>
  );
};

export default App;
