import json
import tkinter as tk
from tkinter import ttk
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from explore import Explore

import customtkinter
import sqlvalidator

import psycopg2
from psycopg2 import extras

customtkinter.set_appearance_mode('light')
customtkinter.set_default_color_theme('green')

MAIN_COLOR = "#212121"
SEC_COLOR = "#373737"
THIRD_COLOR = "#A4A4A4"
FOURTH_COLOR = "#828282"
FONT_COLOR = "#242424"

# Global variable declaration
CONNECTION = None
CONNECTION_NAME = None
EXPLORATION = None
QUERY_TREE = None
QUERY_PLAN = None

class MainApplication(tk.Tk):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.title("Query Explainer")
        self.geometry("1200x800")

        # Main container that will hold the frames
        self.container = ttk.Frame(self)
        self.container.pack(side="top", fill="both", expand=True)

        # Dictionary to store the frames
        self.frames = {}

        # Add frames to the dictionary
        for F in (ConnectionPage, QueryPage):
            frame = F(self.container, self)
            self.frames[F] = frame
            frame.grid(row=0, column=0, sticky="nsew")

        self.show_frame(ConnectionPage)

    def show_frame(self, frame_class):
        if frame_class in self.frames:
        # If the frame exists, destroy it before recreating it
            self.frames[frame_class].destroy()

        # Create a new frame and add it to the dictionary
        frame = frame_class(self.container, self)
        self.frames[frame_class] = frame

        # Position the frame in the container and raise it to the top
        frame.grid(row=0, column=0, sticky="nsew")
        frame.tkraise()

class ConnectionPage(ttk.Frame):

    def __init__(self, parent, controller):
        super().__init__(parent)
        self.entry_widgets = []  # Add this line to store entry widgets

        # create the main frame
        self.main_frame = tk.Frame(self, width=1200, height=800, bg=MAIN_COLOR)
        self.main_frame.pack(side="right", fill="both", expand=True)
        self.main_frame.pack_propagate(0)

        # create the inner container
        self.inner_frame = tk.Frame(self.main_frame, width=600, height=800, bg=MAIN_COLOR)
        self.inner_frame.pack(anchor="center")
        self.inner_frame.pack_propagate(0)

        # add a label to the inner container
        input_label = tk.Label(self.inner_frame, text="Query Juicer", font=("Arial", 54, "bold"),
                               bg=MAIN_COLOR, fg="white")
        input_label.pack(pady=(30, 70))

        # create the inner inner container
        self.inner_inner_frame = tk.Frame(self.inner_frame, width=600, bg=MAIN_COLOR,
                                          highlightbackground=SEC_COLOR, highlightthickness=2)
        self.inner_inner_frame.pack(anchor="center")

        # add a label to the right container
        input_label = tk.Label(self.inner_inner_frame, text="Enter the Connection Details:", font=("Arial", 28, "bold"),
                               bg=MAIN_COLOR, fg="white")
        input_label.pack(pady=(10, 30))

        # create the input rows
        rows = []
        label_names = ['Host IP', "Port Number", "Database Name", 'Username', "Password"]
        for i in range(5):
            # creating a frame for each row of label and input
            row = tk.Frame(self.inner_inner_frame, bg=MAIN_COLOR)
            row.pack(fill="x", padx=20, pady=5)

            # creating a label
            label = tk.Label(row, text=f"{label_names[i] + ':'}", font=("Arial", 28, "bold"), bg=MAIN_COLOR, fg="white",
                             width=15, anchor="w")
            label.pack(side="left")

            # creating an input entry
            entry = customtkinter.CTkEntry(master=row,
                                           width=300,
                                           height=45,
                                           fg_color=SEC_COLOR,
                                           text_color="white",
                                           font=("Arial", 28, "normal"),
                                           border_width=2,
                                           corner_radius=10)
            entry.pack(anchor=tk.E, side="right", fill="x", expand=True)
            self.entry_widgets.append(entry)  # Store the entry widget

            # appending the label and entry to the rows list
            rows.append((label, entry))

        # Now load the connection data
        self.load_connection_data()

        # add a submit button
        submit_button = customtkinter.CTkButton(master=self.inner_inner_frame,
                                                fg_color=SEC_COLOR,
                                                text="Submit",
                                                font=("Arial", 28, "bold"),
                                                hover_color=FOURTH_COLOR,
                                                text_color="white",
                                                command=lambda: self.submit(controller))  # invoking submit_page1
        submit_button.pack(pady=10)

        # center the input rows within the right container
        for row in rows:
            row[0].pack_configure(anchor="center")
            row[1].pack_configure(anchor="center")

        # Error Message
        self.error_message = tk.Label(self.inner_frame, text="",
                                                font=("Arial", 37, "bold"),
                                                bg=MAIN_COLOR, fg="Red")
        self.error_message.pack(anchor="center", pady=(30, 70))

    ### Submit Page
    def submit(self, c):
        # getting the input elements
        entries = self.get_entries()
        # saving the connection details
        connection = {
            "IP": entries[0].get(),
            "Port": entries[1].get(),
            "Database": entries[2].get(),
            "Username": entries[3].get(),
            "Password": entries[4].get(),
        }

        # Write to data.json
        try:
            with open('data.json', 'r') as file:
                data = json.load(file)
        except (FileNotFoundError, json.JSONDecodeError):
            data = {"Connections": [], "Queries": {}}

        # Replace or set the connection details
        data["Connections"] = [connection]

        with open('data.json', 'w') as file:
            json.dump(data, file)

        # Check for empty values in each entries
        for _, value in connection.items():
            if not value: 
                self.error_message.config(text="All fields are required")
                return
        else:
            self.error_message.config(text="")

        print(connection)
        # checking if the connection is valid
        try:
            global EXPLORATION
            EXPLORATION = Explore(database=connection["Database"],
                                  port=connection["Port"],
                                  host=connection["IP"],
                                  user=connection["Username"],
                                  password=connection["Password"])
            # CONNECTION = PostgresDB(connection["IP"], connection["Port"], connection["Database"],
            #                         connection["Username"], connection["Password"])
            global CONNECTION_NAME
            CONNECTION_NAME = connection["IP"]
            print("Connection Successful")
            c.show_frame(QueryPage)
        except:
            self.error_message.config(text="Invalid Connection")
            print("Connection Error")

    def get_entries(self):
        return self.entry_widgets  # Return the stored entry widgets
    
    def load_connection_data(self):
        try:
            with open('data.json', 'r') as file:
                data = json.load(file)
                
                # Check if there are any connections stored
                if data.get("Connections"):
                    last_connection = data["Connections"][-1]  # Load the last connection
                    # Populate the entry fields
                    for i, key in enumerate(["IP", "Port", "Database", "Username", "Password"]):
                        self.entry_widgets[i].insert(0, last_connection.get(key, ""))
                else:
                    print("No connections stored yet.")
        except (FileNotFoundError, json.JSONDecodeError):
            print("Data file not found or invalid format. Creating a new one.")


class QueryPage(ttk.Frame):
    def __init__(self, parent, controller):
        super().__init__(parent)
        self.create_widgets(parent, controller)
        self.load_query_history()

    def create_widgets(self, parent, controller):
        # create the left container (history panel)
        self.left_container = tk.Frame(self, width=300, height=800, bg=SEC_COLOR)
        self.left_container.pack(side="left", fill="both", expand=True)
        self.left_container.pack_propagate(0)

        # create the right container (query input)
        self.right_container = tk.Frame(self, width=900, height=800, bg=MAIN_COLOR)
        self.right_container.pack(side="right", fill="both", expand=True)
        self.right_container.pack_propagate(0)

        # add a label to the left container
        history_label = tk.Label(self.left_container, text="Previous\nQueries", bg=SEC_COLOR, font=("Arial", 32, "bold"), fg="white")
        history_label.pack(pady=10)

        # Frame for listbox and scrollbar
        listbox_frame = tk.Frame(self.left_container, bg=SEC_COLOR)
        listbox_frame.pack(side="left", fill="both", expand=True)

        # create a scrollable listbox widget
        self.history_listbox = tk.Listbox(listbox_frame, bg=SEC_COLOR, font=("Arial", 20), fg=FONT_COLOR, selectmode="single", highlightthickness=0, borderwidth=0)
        self.history_listbox.pack(side="left", fill="both", expand=True)

        # create a scrollbar widget and link it to the listbox
        scrollbar = ttk.Scrollbar(listbox_frame, orient="vertical", command=self.history_listbox.yview)
        scrollbar.pack(side="right", fill="y")
        self.history_listbox.configure(yscrollcommand=scrollbar.set)
        self.history_listbox.bind("<<ListboxSelect>>", self.handle_click_history)

        # create the right inner container
        self.right_inner_container = tk.Frame(self.right_container, width=800, height=800, bg=MAIN_COLOR)
        self.right_inner_container.pack(anchor="center")
        self.right_inner_container.pack_propagate(0)

        # Header for Enter SQL Query
        input_label = tk.Label(self.right_inner_container, text="Enter SQL Query Statement", font=("Arial", 28, "bold"),
                               bg=MAIN_COLOR, fg="white")
        input_label.pack(pady=20)

        # Query Input Box
        self.query_textbox = customtkinter.CTkTextbox(self.right_inner_container,
                                              width=800, height=500,
                                              fg_color="white",  # White Background
                                              text_color="black",  # Black Font color
                                              font=("Courier", 20, "normal"))
        self.query_textbox.pack(padx=10, pady=10)

        # Return to Connection Page Button
        database_back_button = customtkinter.CTkButton(master=self.right_inner_container,
                                                       fg_color=SEC_COLOR,
                                                       text="Back to Connection",
                                                       font=("Arial", 28, "bold"),
                                                       hover_color=FOURTH_COLOR,
                                                       text_color="white",
                                                       command=lambda: controller.show_frame(ConnectionPage))
        database_back_button.pack(side="left", anchor="s", pady=(0, 35))

        # Execute Query button
        execute_button = customtkinter.CTkButton(master=self.right_inner_container,
                                                 fg_color=SEC_COLOR,
                                                 text="Execute",
                                                 font=("Arial", 28, "bold"),
                                                 hover_color=FOURTH_COLOR,
                                                 text_color="white",
                                                 command=lambda: self.execute_query(controller))  # invoking the validation function
        execute_button.pack(side="right", anchor="s", pady=(0, 35))

    def execute_query(self, controller):
        query = self.query_textbox.get("1.0", "end-1c")
        
        # Validate sql query
        query_parse = sqlvalidator.parse(query)
        if query_parse.is_valid():
            try:
                ## Exploration
                global QUERY_PLAN
                QUERY_PLAN = EXPLORATION.explain(query)
                controller.show_frame(QueryResultPage)
                print('Explored query successfully')
            except psycopg2.DatabaseError as e:
                print(f"Database error: {e}")
                tk.messagebox.showerror("Database error", e)
                return
            except Exception as e:
                print(f"An error occurred: {e}")
                tk.messagebox.showerror("An error occurred", e)
                return
        else:
            print("Invalid SQL Query")
            tk.messagebox.showerror("Error", "Invalid SQL Query")
            return

        # Store query inputs to history
        try: 
            with open('data.json', 'r') as fil:
                data = json.load(fil)
                # Initialize 'Queries' for CONNECTION_NAME if not present
                if CONNECTION_NAME not in data["Queries"]:
                    data["Queries"][CONNECTION_NAME] = []
                # Add query to history if it is not empty
                if query.strip():  
                    data["Queries"][CONNECTION_NAME].append(query)
        except FileNotFoundError:
            # Create file and structure if it does not exist
            data = {"Connections": [], "Queries": {CONNECTION_NAME: [query]}}
        
        with open('data.json', 'w+') as fil:
            json.dump(data, fil)      
    
    def load_query_history(self):
        try:
            with open('data.json', 'r') as f:
                data = json.load(f)
                queries = data.get('Queries', {}).get(CONNECTION_NAME, [])
                self.history_listbox.delete(0, tk.END)  # Clear existing items
                for i, query in enumerate(queries):
                    self.history_listbox.insert("end", "   " + f"{query}")
                    if i % 2 == 0:
                        self.history_listbox.itemconfig(i, {'bg': THIRD_COLOR})
                    else:
                        self.history_listbox.itemconfig(i, {'bg': FOURTH_COLOR})
        except FileNotFoundError:
            print('No query history available')

    def handle_click_history(self, event):
        selection = event.widget.curselection()
        if selection:
            index = selection[0]
            query = event.widget.get(index)[3:]

            self.query_textbox.delete("1.0", "end")  # Delete current content
            self.query_textbox.insert("1.0", query)  # Insert new content

class QueryResultPage(ttk.Frame):
    def __init__(self, parent, controller):
        super().__init__(parent)

        qep_tree = QUERY_PLAN.save_graph_file()
        explanation = QUERY_PLAN.create_explanation(QUERY_PLAN.root)
        totalCost = QUERY_PLAN.calculate_total_cost()
        
        global QUERY_TREE
        QUERY_TREE = qep_tree

        self.create_widgets(parent, controller)

        self.insert_formatted_text(explanation)
        self.total_cost_span.config(text=f"Total Cost: {totalCost}")

    def create_widgets(self, parent, controller):
        # Main Frame
        container = tk.Frame(self, width=1200, height=800, bg=MAIN_COLOR)
        container.pack(side="left", fill="both", expand=True)
        container.pack_propagate(0)

        # Header and Button Container
        header_container = tk.Frame(container, bg=MAIN_COLOR)
        header_container.pack(fill="x", pady=20)

        # Header
        header = tk.Label(header_container, text="Query Visualisation", font=("Arial", 28, "bold"), bg=MAIN_COLOR, fg="white")
        header.pack()

        # Return Button
        database_back_button = customtkinter.CTkButton(master=header_container,
                                                       fg_color=SEC_COLOR,
                                                       text="Enter Another Query",
                                                       font=("Arial", 28, "bold"),
                                                       hover_color=FOURTH_COLOR,
                                                       text_color="white",
                                                       command=lambda: controller.show_frame(QueryPage))
        database_back_button.pack(side="right", padx=30)

        # Content Frame
        content_frame = tk.Frame(container, bg=MAIN_COLOR)
        content_frame.pack(fill="both", expand=True, padx=10, pady=10)

        # Canvas for Scrolling
        canvas = tk.Canvas(content_frame, bg=MAIN_COLOR)
        canvas.pack(side="left", fill="both", expand=True)

        # Scrollbar for Canvas
        scrollbar = ttk.Scrollbar(content_frame, orient="vertical", command=canvas.yview)
        scrollbar.pack(side="right", fill="y")

        # Configure Canvas Scrolling
        canvas.configure(yscrollcommand=scrollbar.set)
        canvas.bind("<Configure>", lambda e: canvas.configure(scrollregion=canvas.bbox("all")))

        # Frame inside Canvas (for actual content)
        scrollable_frame = tk.Frame(canvas, bg=MAIN_COLOR)
        canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")

        # Scroll Behaviour
        def on_mousewheel(event):
            canvas.yview_scroll(int(-1*(event.delta/120)), "units")
        canvas.bind_all("<MouseWheel>", on_mousewheel)  # For Windows

        # Query Explanation Section ----------------
        explanation_title = tk.Label(scrollable_frame, text="Query Explanation", font=("Arial", 24, "bold"), bg=MAIN_COLOR, fg="white")
        explanation_title.pack(anchor="w", padx=50, pady=10)

        self.exploration_text = tk.Text(scrollable_frame, 
                                        width=100, height=20,
                                        bg="white", fg="black",
                                        font=("Courier", 12, "normal"))
        self.exploration_text.pack(fill="both", expand=True, padx=50, pady=20)

        # QEP
        # Embed the plot in the Tkinter window
        qep_title = tk.Label(scrollable_frame, text="Query Execution Plan", font=("Arial", 24, "bold"), bg=MAIN_COLOR, fg="white")
        qep_title.pack(anchor="w", padx=50, pady=20)

        qep_canvas = FigureCanvasTkAgg(QUERY_TREE, master=scrollable_frame)
        qep_canvas.draw()
        qep_canvas.get_tk_widget().pack(side=tk.TOP, fill=tk.BOTH, expand=True, padx=50, pady=20)

        # Total Cost Section
        self.total_cost_span = tk.Label(scrollable_frame, text="", font=("Arial", 24, "bold"), bg=MAIN_COLOR, fg="white")
        self.total_cost_span.pack(anchor="w", padx=50, pady=20)

    def insert_formatted_text(self, explanation):
        self.exploration_text.configure(state="normal")  # Enable the textbox
        index = 1
        for statement in explanation:
            statement = statement.replace("<b>", "").replace("</b>", "")
            statement = f"Step {index}: {statement}"
            self.exploration_text.insert("end", statement + "\n\n")
            index += 1
        self.exploration_text.configure(state="disabled")  # Disable the textbox