import { Input, List, Collapse } from "antd";
import { UserOutlined } from "@ant-design/icons";
import { useState, useRef } from "react";
import { Pagination } from "antd";
const parse = require("html-react-parser");

const axios = require("axios");
const { Panel } = Collapse;

const { Search } = Input;

const SearchBar = () => {
  const [search_results, setSearchResults] = useState([]);
  const [total_results, setTotalResults] = useState(-1);
  const searchRef = useRef();
  const [curr_page, setCurrPage] = useState(1);
  const [curr_page_size, setCurrPageSize] = useState(10);
  const onSearch = (value, page = 1, page_size = curr_page_size) => {
    page = page || 1;
    page_size = page_size || 10;
    console.log("page", page);
    axios
      .post(
        "http://localhost:9243/ap_dataset_stem_stop/_search",
        {
          query: {
            query_string: {
              query: value,
            },
          },
          size: page_size,
          from: page - 1,
          highlight: {
            fields: {
              text: {},
            },
          },
        },
        { headers: { "Access-Control-Allow-Origin": "*" } }
      )
      .then(function (response) {
        console.log("response", response);
        setTotalResults(response["data"]["hits"]["total"]["value"]);
        setSearchResults(response["data"]["hits"]["hits"]);
      })
      .catch(function (error) {
        setSearchResults([]);
        setTotalResults(0);
      });
  };
  const onChange = (page, pageSize) => {
    setCurrPage(page);
    setCurrPageSize(pageSize);
    onSearch(searchRef.current.state.value, page, pageSize);
  };
  return (
    <>
      <Search
        ref={searchRef}
        size="large"
        placeholder="Search query..."
        enterButton
        onSearch={(value) => onSearch(value, 1, curr_page_size)}
      />
      {total_results >= 0 && (
        <>
          <Pagination
            className="pagination"
            onChange={onChange}
            showTotal={(total) => `Total ${total} items`}
            defaultPageSize={10}
            defaultCurrent={1}
            total={total_results}
          />
          <List
            itemLayout="horizontal"
            dataSource={search_results}
            renderItem={(item) => (
              <List.Item className="search-result">
                <List.Item.Meta
                  title={
                    <a href={item._id}>
                      <span className="search-item">
                        {item._source.title ? item._source.title : item._id}
                      </span>
                    </a>
                  }
                  description={
                    <>
                      <div className="search-url"> {item._id} </div>
                      <div className="search-highlight">
                        {item.highlight && item.highlight.text && (
                          <div
                            dangerouslySetInnerHTML={{
                              __html: item.highlight.text,
                            }}
                          />
                        )}
                        <i>Author: {item._source.author}</i>
                      </div>
                    </>
                  }
                />
              </List.Item>
            )}
          />
        </>
      )}
    </>
  );
};
export default SearchBar;
